package main

import (
	"log"
	"os"
	"video-watermark-ffmpeg/logo"
)

func main() {
	//videoUrl := "https://cdn.google.live/video/75an099hrpb6od35elqopnceah-1670826684781993072384.mp4"
	//logoUrl := "logo.png"
	//log.Println("#####start video url=" + videoUrl)
	//newSavePath := "/tmp/" + path.Base(videoUrl)
	//cmd := exec.Command("ffmpeg", "-i", videoUrl, "-i", logoUrl, "-filter_complex", "overlay=10:10", newSavePath)
	//var out bytes.Buffer
	//var stderr bytes.Buffer
	//cmd.Stdout = &out
	//cmd.Stderr = &stderr
	//err := cmd.Run()
	//if err != nil {
	//	log.Println("ffmpeg run error "+videoUrl, err, stderr.String())
	//}
	//log.Println(newSavePath)

}

func testAddTextToLogo() {
	imgSource, err := os.Open("./assets/video-logo.png")
	if err != nil {
		log.Println(err)
	}
	defer imgSource.Close()
	t := logo.TextInfo{
		Text: "@testUsername",
		Size: 15,
		//Color:   "#FF0000",
		YOffset: 6,
	}
	t.XOffset = len(t.Text) * 6

	t.AddTextToLogo(imgSource)
}
