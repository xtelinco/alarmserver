package hikvision

import (
	"encoding/xml"
	"fmt"
	"github.com/icholy/digest"
	"io"
	"os"
	"strings"
	"bytes"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
)

type HttpEventReader struct {
	Debug  bool
	client *http.Client
}

func Progress() *progress {
	fp, err := os.Create("/out/camera.xml")
	if err != nil {
		panic(err)
	}
	return &progress{0, fp}
}

type progress struct {
	total uint64
	f *os.File
}

func (p *progress) Write(b []byte) (int, error) {
	_, err := p.f.Write(b)
	if err != nil {
		panic(err)
	}
	return len(b), nil
}

func BoundaryFilter(r io.Reader, boundary string) io.Reader{
	b := make([]byte, len(boundary)+2)
	copy(b, "--")
	copy(b[2:], boundary)
	return &boundaryFilter{r, b, true}
}

type boundaryFilter struct {
	r io.Reader
	boundary []byte
	hasEndBoundary bool
}


// This Hikvision camera has a bug in that it sends an extra boundary at the end of
// its linedetect alarm, after it has sent the image. This code detects that it has
// already sent the boundary and removes the boundary from the next buffer
func(t *boundaryFilter) Read(p []byte) (n int, err error) {
	buf := make([]byte, len(p))
	n, err = t.r.Read(buf)
	if n > 0 {
		i := 0
		for {
			pos1 := bytes.Index(buf[i:], t.boundary)
			if pos1 == 0 && t.hasEndBoundary {
				copy(buf[i:], buf[i+len(t.boundary)+2:])
				n -= len(t.boundary) + 2
				//fmt.Println("remove boundary from start, we sent it at the end of last buffer")
				t.hasEndBoundary = false
				continue
			} else if pos1 >= 0 {
				pos2 := bytes.Index(buf[i+pos1+len(t.boundary):], t.boundary)
				//fmt.Printf("Found boundary at %d %d\n", pos1, pos2)
				if pos2 >= 0 {
					pos2 += i + pos1 + len(t.boundary)
					//fmt.Printf("i:%d pos1:%d pos2: %d len:%d n-pos2-len:%d\n", i, pos1, pos2, len(t.boundary), n - pos2 - len(t.boundary))
					t.hasEndBoundary = n - pos2 - len(t.boundary) <= 2
				}
			}
			break
		}
		copy(p, buf[:n])
	}
	return n, err
}


func (eventReader *HttpEventReader) ReadEvents(camera *HikCamera, channel chan<- HikEvent, callback func()) {
	if eventReader.client == nil {
		eventReader.client = &http.Client{}
		if camera.AuthMethod == Digest {
			eventReader.client.Transport = &digest.Transport{
				Username: camera.Username,
				Password: camera.Password,
			}
		}
	}

	request, err := http.NewRequest("GET", camera.Url+"Event/notification/alertStream", nil)
	if err != nil {
		fmt.Printf("HIK: Error: Could not connect to camera %s\n", camera.Name)
		fmt.Println("HIK: Error", err)
		callback()
		return
	}
	if camera.AuthMethod == Basic {
		request.SetBasicAuth(camera.Username, camera.Password)
	}

	response, err := eventReader.client.Do(request)
	if err != nil {
		fmt.Printf("HIK: Error opening HTTP connection to camera %s\n", camera.Name)
		fmt.Println(err)
		return
	}

	if response.StatusCode != 200 {
		fmt.Printf("HIK: BAD STATUS %d", response.StatusCode)
	}
	defer response.Body.Close()

	//tee := io.TeeReader(response.Body, Progress())

	// FIGURE OUT MULTIPART BOUNDARY
	mediaType, params, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaType != "multipart/mixed" || params["boundary"] == "" {
		fmt.Println("HIK: ERROR: Camera " + camera.Name + " does not seem to support event streaming")
		fmt.Println("            Is it a doorbell? Try adding rawTcp to its config!")
		callback()
		return
	}
	multipartBoundary := params["boundary"] 

	xmlEvent := XmlEvent{}

	// READ PART BY PART
	multipartReader := multipart.NewReader(BoundaryFilter(response.Body, multipartBoundary), multipartBoundary)
	for {
		part, err := multipartReader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			if strings.Contains(err.Error(),"connection reset by peer") {
				break
			}
			fmt.Println(err)
			continue
		}
		contentType := part.Header.Get("Content-Type")
		contentLength, _ := strconv.Atoi(part.Header.Get("Content-Length"))
		body := make([]byte, contentLength)
		pos := 0
		n := 0
		for pos < contentLength {
			n, err = part.Read(body[pos:])
			pos += n
			if err != nil {
				break
			}
		}
		if pos + 2 < contentLength {
			continue
		}

		if strings.Contains(contentType, "image/jpeg") {
			if camera.PublishImages {
				_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
				if err == nil {
					filename, ok := params["filename"]
					if ok {
						if eventReader.Debug {
							fmt.Println("HIK: Sending an image: " + filename)
						}
						event := HikEvent{Camera: camera}
						event.Type = "image/" + filename
						event.Message = body
						channel <- event
					}
				}
			}
			continue
		}

		if strings.Contains(contentType, "application/xml") {
			err = xml.Unmarshal(body, &xmlEvent)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// FILL IN THE CAMERA INTO FRESHLY-UNMARSHALLED EVENT
			xmlEvent.Camera = camera

			if eventReader.Debug {
				log.Printf("%s event: %s (%s - %d)", xmlEvent.Camera.Name, xmlEvent.Type, xmlEvent.State, xmlEvent.Id)
			}

			switch xmlEvent.State {
			case "active":
				if eventReader.Debug {
					fmt.Println("HIK: SENDING CAMERA EVENT!")
				}
				event := HikEvent{Camera: camera}
				event.Type = xmlEvent.Type
				if camera.SendXML {
					event.Message = string(body)
				} else {
					event.Message = xmlEvent.Description
				}
				channel <- event
				xmlEvent.Active = true
			case "inactive":
				xmlEvent.Active = false
			}
		}
	}
}
