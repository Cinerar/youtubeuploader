/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/porjo/go-flowrate/flowrate"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"google.golang.org/api/gensupport"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/youtube/v3"
)

var (
	filename     = flag.String("filename", "", "Filename to upload. Can be a URL")
	thumbnail    = flag.String("thumbnail", "", "Thumbnail to upload. Can be a URL")
	title        = flag.String("title", "Video Title", "Video title")
	description  = flag.String("description", "uploaded by youtubeuploader", "Video description")
	categoryId   = flag.String("categoryId", "", "Video category Id")
	tags         = flag.String("tags", "", "Comma separated list of video tags")
	privacy      = flag.String("privacy", "private", "Video privacy status")
	quiet        = flag.Bool("quiet", false, "Suppress progress indicator")
	rate         = flag.Int("ratelimit", 0, "Rate limit upload in kbps. No limit by default")
	limitBetween = flag.String("limitBetween", "00:00-23:59", "Only rate limit between these times (local time zone)")
	metaJSON     = flag.String("metaJSON", "", "JSON file containing title,description,tags etc (optional)")
	headlessAuth = flag.Bool("headlessAuth", false, "set this if host does not have browser available for oauth authorisation step")
	resume       = flag.Bool("resume", false, "Try to resume the previous upload in the case of a power failure or the program crashing")
)

type VideoMeta struct {
	// snippet
	Title       string   `json:"title,omitempty"`
	Description string   `json:"description,omitempty"`
	CategoryId  string   `json:"categoryId,omitempty"`
	Tags        []string `json:"tags,omitempty"`

	// status
	PrivacyStatus       string `json:"privacyStatus,omitempty"`
	Embeddable          bool   `json:"embeddable,omitempty"`
	License             string `json:"license,omitempty"`
	PublicStatsViewable bool   `json:"publicStatsViewable,omitempty"`
	PublishAt           string `json:"publishAt,omitempty"`

	// recording details
	Location            *youtube.GeoPoint `json:"location,omitempty"`
	LocationDescription string            `json:"locationDescription, omitempty"`
	RecordingDate       Date              `json:"recordingDate, omitempty"`

	PlaylistID string `json:"playlistId, omitempty"`
}

const inputTimeLayout = "15:04"
const inputDateLayout = "2006-01-02"
const outputDateLayout = "2006-01-02T15:04:05.000Z" //ISO 8601 (YYYY-MM-DDThh:mm:ss.sssZ)

type Date struct {
	time.Time
}

func main() {
	flag.Parse()

	if *filename == "" {
		fmt.Printf("You must provide a filename of a video file to upload\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	limitTimes, err := parseLimitBetween(*limitBetween)
	if err != nil {
		fmt.Printf("Invalid value for -limitBetween: %v", err)
		flag.PrintDefaults()
		os.Exit(1)
	}

	reader, reopen, filesize := Open(*filename)
	defer reader.Close()

	var thumbReader io.ReadCloser
	if *thumbnail != "" {
		thumbReader, _, _ = Open(*thumbnail)
		defer thumbReader.Close()
	}

	ctx := context.Background()
	transport := &limitTransport{rt: http.DefaultTransport, times: limitTimes, filesize: filesize}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
		Transport: transport,
	})

	var quitChan chan chan struct{}

	if !*quiet {
		ticker := time.Tick(time.Second)
		quitChan = make(chan chan struct{})
		go func() {
			var erase int
			for {
				select {
				case <-ticker:
					if transport.reader != nil {
						s := transport.reader.Monitor.Status()
						curRate := float32(s.CurRate)
						var status string
						if curRate >= 125000 {
							status = fmt.Sprintf("Progress: %8.2f Mbps, %d / %d (%s) ETA %8s", curRate/125000, filesize-s.BytesRem, filesize, s.Progress, s.TimeRem)
						} else {
							status = fmt.Sprintf("Progress: %8.2f kbps, %d / %d (%s) ETA %8s", curRate/125, filesize-s.BytesRem, filesize, s.Progress, s.TimeRem)
						}
						fmt.Printf("\r%s\r%s", strings.Repeat(" ", erase), status)
						erase = len(status)
					}
				case ch := <-quitChan:
					close(ch)
					return
				}
			}
		}()
	}
	client, err := buildOAuthHTTPClient(ctx, youtube.YoutubeUploadScope)
	if err != nil {
		log.Fatalf("Error building OAuth client: %v", err)
	}

	upload := &youtube.Video{
		Snippet:          &youtube.VideoSnippet{},
		RecordingDetails: &youtube.VideoRecordingDetails{},
		Status:           &youtube.VideoStatus{},
	}

	videoMeta := LoadVideoMeta(*metaJSON, upload)

	service, err := youtube.New(client)
	if err != nil {
		log.Fatalf("Error creating playlist service: %s", err)
	}

	if upload.Status.PrivacyStatus == "" {
		upload.Status.PrivacyStatus = *privacy
	}
	if upload.Snippet.Tags == nil && strings.Trim(*tags, "") != "" {
		upload.Snippet.Tags = strings.Split(*tags, ",")
	}
	if upload.Snippet.Title == "" {
		upload.Snippet.Title = *title
	}
	if upload.Snippet.Description == "" {
		upload.Snippet.Description = *description
	}
	if upload.Snippet.CategoryId == "" && *categoryId != "" {
		upload.Snippet.CategoryId = *categoryId
	}

	fmt.Printf("Uploading file '%s'...\n", *filename)

	video, err := ResumableUpload(service, "snippet,status,recordingDetails", upload, reader, reopen, filesize, ctx, client)

	quit := make(chan struct{})
	quitChan <- quit
	<-quit

	if err != nil {
		if video != nil {
			log.Fatalf("Error making YouTube API call: %v, %v", err, video.HTTPStatusCode)
		} else {
			log.Fatalf("Error making YouTube API call: %v", err)
		}
	}
	fmt.Printf("\nUpload successful! Video ID: %v\n", video.Id)

	if videoMeta.PlaylistID != "" {
		err = AddVideoToPlaylist(service, videoMeta.PlaylistID, video.Id)
		if err != nil {
			log.Fatalf("Error adding video to playlist: %s", err)
		}
	}
	if thumbReader != nil {
		log.Printf("Uploading thumbnail '%s'...\n", *thumbnail)
		_, err = service.Thumbnails.Set(video.Id).Media(thumbReader).Do()
		if err != nil {
			log.Fatalf("Error making YouTube API call: %v", err)
		}
		fmt.Printf("Thumbnail uploaded!\n")
	}
}

var ErrColdResume = errors.New("[resuming from previous session - this error should not be displayed]")

func ResumableUpload(service *youtube.Service, parts string, video *youtube.Video, reader io.Reader, reopenReader func(int64) (io.ReadCloser, error), filesize int64, ctx context.Context, client *http.Client) (*youtube.Video, error) {
	var resumableSession string
	if *resume {
		session, err := ioutil.ReadFile(".youtubeuploader-resume")
		if err == nil {
			resumableSession = string(session)
		}
	}

	reader1, contentType := gensupport.DetermineContentType(reader, "")

	userAgent := googleapi.UserAgent + " (+https://github.com/porjo/youtubeuploader)"

	var req *http.Request
	var resp *http.Response
	var err error

	if resumableSession == "" {
		query := url.Values{
			"part":       {parts},
			"uploadType": {"resumable"},
		}

		url := strings.Replace(googleapi.ResolveRelative(service.BasePath, "videos"), "https://www.googleapis.com/", "https://www.googleapis.com/upload/", 1) + "?" + query.Encode()

		var body []byte
		body, err = json.Marshal(video)
		if err != nil {
			return nil, err
		}

		req, err = http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")
		req.Header.Set("X-Upload-Content-Length", fmt.Sprintf("%d", filesize))
		req.Header.Set("X-Upload-Content-Type", contentType)

		resp, err = gensupport.SendRequest(ctx, client, req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			return nil, fmt.Errorf("unexpected return status from YouTube API: %s", resp.Status)
		}

		resumableSession = resp.Header.Get("Location")
		if *resume {
			err = ioutil.WriteFile(".youtubeuploader-resume", []byte(resumableSession), 0644)
			if err != nil {
				log.Println("warning: failed to save session information:", err)
			}
		}

		// First attempt: just run the request as normal
		req, err = http.NewRequest("PUT", resumableSession, reader1)
		req.ContentLength = filesize
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Length", fmt.Sprintf("%d", filesize))
		req.Header.Set("Content-Type", contentType)

		resp, err = gensupport.SendRequest(ctx, client, req)
	} else {
		err = ErrColdResume
	}

	minBackoff, maxBackoff := 500*time.Millisecond, 15*time.Minute
	backoff := minBackoff

	// Retry as many times as needed
	for err != nil || resp.StatusCode >= 400 {
		if err != ErrColdResume {
			if err != nil {
				log.Println("video upload request failed temporarily with error:", err)
			} else {
				resp.Body.Close()

				// Handle permanent errors
				if resp.StatusCode >= 400 &&
					resp.StatusCode != http.StatusInternalServerError &&
					resp.StatusCode != http.StatusBadGateway &&
					resp.StatusCode != http.StatusServiceUnavailable &&
					resp.StatusCode != http.StatusGatewayTimeout {
					if *resume {
						os.Remove(".youtubeuploader-resume")
					}
					return nil, fmt.Errorf("YouTube API responded to the video upload request with %s", resp.Status)
				}

				log.Println("video upload request failed temporarily with http status:", resp.Status)
			}

			log.Println("waiting", backoff, "and trying again")
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		req, err = http.NewRequest("PUT", resumableSession, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", filesize))
		req.Header.Set("X-GUploader-No-308", "yes")

		resp, err = gensupport.SendRequest(ctx, client, req)
		if err != nil {
			continue
		}
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if t, err := http.ParseTime(retryAfter); err == nil {
				backoff = time.Until(t)
				continue
			}
			if s, err := strconv.ParseInt(retryAfter, 10, 64); err == nil {
				backoff = time.Second * time.Duration(s)
				continue
			}
		}
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Http-Status-Code-Override") != "308" {
			continue
		}

		// Resume incomplete
		resp.Body.Close()
		backoff = minBackoff
		var start int64
		if byteRange := resp.Header.Get("Range"); strings.HasPrefix(byteRange, "bytes=0-") {
			start, err = strconv.ParseInt(strings.TrimPrefix(byteRange, "bytes=0-"), 10, 64)
			if err == nil {
				start++
			} else {
				start = 0
			}
		}

		var reader2 io.ReadCloser
		reader2, err = reopenReader(start)
		if err != nil {
			log.Println("failed to open reader")
			continue
		}

		log.Println("resuming from byte", start, "of", filesize)

		req, err = http.NewRequest("PUT", resumableSession, reader2)
		req.ContentLength = filesize - start
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Content-Length", strconv.FormatInt(filesize-start, 10))
		req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, filesize-1, filesize))

		resp, err = gensupport.SendRequest(ctx, client, req)
		reader2.Close()
	}

	// Request was successful.
	defer resp.Body.Close()

	if *resume {
		os.Remove(".youtubeuploader-resume")
	}

	response := &youtube.Video{
		ServerResponse: googleapi.ServerResponse{
			HTTPStatusCode: resp.StatusCode,
			Header:         resp.Header,
		},
	}
	err = json.NewDecoder(resp.Body).Decode(response)
	return response, err
}

type limitTransport struct {
	rt       http.RoundTripper
	times    [2]int
	reader   *flowrate.Reader
	filesize int64
}

func (t *limitTransport) RoundTrip(r *http.Request) (res *http.Response, err error) {
	// FIXME need a better way to detect which roundtrip is the media upload
	if r.ContentLength > 1000 {
		var monitor *flowrate.Monitor

		if t.reader != nil {
			monitor = t.reader.Monitor
		}

		// limit is set in limitChecker.Read
		t.reader = flowrate.NewReader(r.Body, 0)

		if monitor != nil {
			// carry over stats to new limiter
			monitor.SetTransferSize(monitor.Status().Bytes + r.ContentLength)
			t.reader.Monitor = monitor
		} else {
			t.reader.Monitor.SetTransferSize(r.ContentLength)
		}
		r.Body = &limitChecker{t.times, t.reader}
	}

	return t.rt.RoundTrip(r)
}

func (d *Date) UnmarshalJSON(b []byte) (err error) {
	s := string(b)
	s = s[1 : len(s)-1]
	d.Time, err = time.Parse(inputDateLayout, s)
	return
}

func AddVideoToPlaylist(service *youtube.Service, playlistID, videoID string) (err error) {
	listCall := service.Playlists.List("snippet,contentDetails")
	listCall = listCall.Mine(true)
	response, err := listCall.Do()
	if err != nil {
		return fmt.Errorf("error retrieving playlists: %s", err)
	}

	var playlist *youtube.Playlist
	for _, pl := range response.Items {
		if pl.Id == playlistID {
			playlist = pl
			break
		}
	}

	// TODO: handle creation of playlist
	if playlist == nil {
		return fmt.Errorf("playlist ID '%s' doesn't exist", playlistID)
	}

	playlistItem := &youtube.PlaylistItem{}
	playlistItem.Snippet = &youtube.PlaylistItemSnippet{PlaylistId: playlist.Id}
	playlistItem.Snippet.ResourceId = &youtube.ResourceId{
		VideoId: videoID,
		Kind:    "youtube#video",
	}

	insertCall := service.PlaylistItems.Insert("snippet", playlistItem)
	_, err = insertCall.Do()
	if err != nil {
		return fmt.Errorf("error inserting video into playlist: %s", err)
	}

	fmt.Printf("Video added to playlist '%s' (%s)\n", playlist.Snippet.Title, playlist.Id)

	return nil
}

func LoadVideoMeta(filename string, video *youtube.Video) (videoMeta VideoMeta) {
	// attempt to load from meta JSON, otherwise use values specified from command line flags
	if filename != "" {
		file, e := ioutil.ReadFile(filename)
		if e != nil {
			fmt.Printf("Could not read filename file '%s': %s\n", filename, e)
			fmt.Println("Will use command line flags instead")
			goto errJump
		}

		e = json.Unmarshal(file, &videoMeta)
		if e != nil {
			fmt.Printf("Could not read filename file '%s': %s\n", filename, e)
			fmt.Println("Will use command line flags instead")
			goto errJump
		}

		video.Snippet.Tags = videoMeta.Tags
		video.Snippet.Title = videoMeta.Title
		video.Snippet.Description = videoMeta.Description
		video.Snippet.CategoryId = videoMeta.CategoryId

		video.Status.License = videoMeta.License
		video.Status.Embeddable = videoMeta.Embeddable
		video.Status.PublicStatsViewable = videoMeta.PublicStatsViewable

		if videoMeta.PrivacyStatus != "" {
			video.Status.PrivacyStatus = videoMeta.PrivacyStatus
			video.Status.PublishAt = videoMeta.PublishAt
		}
		if videoMeta.Location != nil {
			video.RecordingDetails.Location = videoMeta.Location
		}
		if videoMeta.LocationDescription != "" {
			video.RecordingDetails.LocationDescription = videoMeta.LocationDescription
		}
		if !videoMeta.RecordingDate.IsZero() {
			video.RecordingDetails.RecordingDate = videoMeta.RecordingDate.Format(outputDateLayout)
		}
	}
errJump:

	if video.Status.PrivacyStatus == "" {
		video.Status = &youtube.VideoStatus{PrivacyStatus: *privacy}
	}
	if video.Snippet.Tags == nil && strings.Trim(*tags, "") != "" {
		video.Snippet.Tags = strings.Split(*tags, ",")
	}
	if video.Snippet.Title == "" {
		video.Snippet.Title = *title
	}
	if video.Snippet.Description == "" {
		video.Snippet.Description = *description
	}
	if video.Snippet.CategoryId == "" && *categoryId != "" {
		video.Snippet.CategoryId = *categoryId
	}

	return
}

func Open(filename string) (reader io.ReadCloser, reopen func(int64) (io.ReadCloser, error), filesize int64) {
	if strings.HasPrefix(filename, "http") {
		resp, err := http.Head(filename)
		if err != nil {
			log.Fatalf("Error opening %v: %v", filename, err)
		}
		lenStr := resp.Header.Get("content-length")
		if lenStr != "" {
			filesize, err = strconv.ParseInt(lenStr, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
		}

		resp, err = http.Get(filename)
		if err != nil {
			log.Fatalf("Error opening %v: %v", filename, err)
		}
		if resp.ContentLength != 0 {
			filesize = resp.ContentLength
		}
		reader = resp.Body
		reopen = func(offset int64) (io.ReadCloser, error) {
			resp, err := http.Get(filename)
			if err != nil {
				return nil, err
			}
			_, err = io.CopyN(ioutil.Discard, resp.Body, offset)
			if err != nil {
				resp.Body.Close()
				return nil, err
			}
			return resp.Body, nil
		}
		return
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening %v: %v", filename, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Error stating file %v: %v", filename, err)
	}

	return file, func(offset int64) (io.ReadCloser, error) {
		_, err := file.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		return ioutil.NopCloser(file), nil
	}, fileInfo.Size()
}

func parseLimitBetween(between string) ([2]int, error) {
	parts := strings.Split(between, "-")
	if len(parts) != 2 {
		return [2]int{}, fmt.Errorf("limitBetween should have 2 parts separated by a hyphen")
	}

	start, err := time.ParseInLocation(inputTimeLayout, parts[0], time.Local)
	if err != nil {
		return [2]int{}, fmt.Errorf("limitBetween start time was invalid: %v", err)
	}

	end, err := time.ParseInLocation(inputTimeLayout, parts[1], time.Local)
	if err != nil {
		return [2]int{}, fmt.Errorf("limitBetween end time was invalid: %v", err)
	}

	sh, sm, _ := start.Clock()
	eh, em, _ := end.Clock()
	return [2]int{sh*60 + sm, eh*60 + em}, nil
}

type limitChecker struct {
	limits [2]int
	reader *flowrate.Reader
}

func (lc *limitChecker) Read(p []byte) (n int, err error) {
	h, m, _ := time.Now().Clock()

	// minutes since start of rate limit period
	start := h*60 + m - lc.limits[0]
	if start < 0 {
		start += 24 * 60
	}

	// minutes since end of rate limit period
	end := h*60 + m - lc.limits[1]
	if end < 0 {
		end += 24 * 60
	}

	if start > end {
		lc.reader.SetLimit(0)
	} else {
		// kbit/s to B/s = 1000/8 = 125
		lc.reader.SetLimit(int64(*rate * 125))
	}

	return lc.reader.Read(p)
}

func (lc *limitChecker) Close() error {
	return nil
}
