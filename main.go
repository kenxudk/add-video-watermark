package main

import (
	"bytes"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"net/url"
	"strings"
	"video-watermark-ffmpeg/logo"

	"log"
	"os"
	"os/exec"
	"path"
)

//doc : https://docs.aws.amazon.com/zh_cn/lambda/latest/dg/golang-package.html#golang-package-libraries
//ffmpeg aws lambda layer 层： https://github.com/rpidanny/ffmpeg-lambda-layer

// 水印在s3里面存放的前缀目录，比如以前的key=feed/sss.jpg ; 需要拼接为 key=watermark/feed/sss.jpg
// The prefix directory where the watermark is stored in s3, such as the previous key=feed/sss.jpg; It needs to be spliced as key=watermark/feed/sss.jpg
var watermarkPrefixPath = "watermark/"

// 视频下载和生成的存放目录
// Storage directory for video download and generation
var savePath = "/tmp"

// RequestData 请求的json数据
type RequestData struct {
	Channel   string `json:"channel"`
	Name      string `json:"name"`
	Key       string `json:"key"`
	FileW     int    `json:"file_w"`    //key 宽
	FileH     int    `json:"file_h"`    //key 高
	FontSize  string `json:"fontsize"`  //字体大小。默认15
	FontColor string `json:"fontcolor"` //字体颜色，默认白色
}

var (
	s3Bucket = ""
	awsAk    = ""
	awsSk    = ""
	//cdnDomain = "" //域名 domain
	awsRegion = "" //region
)

// ResponseData 返回数据
type ResponseData struct {
	Body BodyData `json:"body"`
}
type BodyData struct {
	Data string `json:"data"`
}

func init() {
	//获取s3桶名
	s3Bucket = os.Getenv("Bucket")
	if s3Bucket == "" {
		//你还没有配置S3的桶名
		log.Fatalln("You haven't configured S3 bucket name")
		return
	}
	awsAk = os.Getenv("AwsAccessKey")
	if awsAk == "" {
		//你还没有aws ak
		log.Fatalln("You haven't aws ak yet")
		return
	}
	awsSk = os.Getenv("AwsSecretKey")
	if awsSk == "" {
		//你还没有aws sk
		log.Fatalln("You don't have aws sk")
		return
	}
	//cdnDomain = os.Getenv("CdnDomain")
	//if cdnDomain == "" {
	//	return
	//}
	awsRegion = os.Getenv("AwsRegion")
	if awsRegion == "" {
		//你还没有配置aws region
		log.Fatalln("You haven't configured aws region")
		return
	}
}

func main() {
	lambda.Start(HandleLambdaEvent)
}

func HandleLambdaEvent(event RequestData) (ResponseData, error) {
	//完整的视频URL
	key := event.Key
	username := event.Name
	log.Println("#####start video url=" + key + ",name=" + username)
	logoUrl := savePath + "/assets/video-logo.png"
	if len(username) > 0 {
		//logo下面有文字需要生成新的logo图片
		t := logo.TextInfo{Text: username, Size: 15, YOffset: 6}
		t.XOffset = len(t.Text) * 6
		videoImgSource, err := os.Open("./assets/video-logo.png")
		if err != nil {
			log.Fatalln("assets/video-logo.png open fail", err)
		}
		defer videoImgSource.Close()
		//获取添加文字后的url路径
		logoUrl = t.AddTextToLogo(videoImgSource)
		if event.FileW > 0 && event.FileH > 0 {
			//按比例缩小
			logoUrl, err = logo.PngResize(logoUrl, event.FileH)
			if err != nil {
				log.Println("logo.PngResize fail", err)
			}
		}
	}
	log.Println("(1)logo path:" + logoUrl)
	pathBase := path.Base(key)
	newSavePath := savePath + "/" + pathBase
	cmd := exec.Command("ffmpeg", "-i", key, "-i", logoUrl, "-filter_complex", "overlay=10:10", "-y", newSavePath)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Println(stderr.String())
		log.Println("ffmpeg run error: " + key)
		log.Fatalln(err)
	}
	log.Println("(2)watermark file path : " + newSavePath)
	urlS3Key := getS3Key(key, pathBase)
	newKey := upLoadToAwsS3(newSavePath, urlS3Key)
	return ResponseData{Body: BodyData{Data: newKey}}, nil
}

/**
 *  上传到s3
 *  Upload to s3
 */
func upLoadToAwsS3(newSavePath string, oldS3Key string) string {
	//s3
	key := oldS3Key
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:      aws.String(awsRegion),
			Credentials: credentials.NewStaticCredentials(awsAk, awsSk, ""),
		}),
	)
	fb, err3 := os.Open(newSavePath)
	if err3 != nil {
		log.Fatalln("open watermark video error, "+newSavePath+" ;  ", err3)
	}
	defer fb.Close()
	newKey := watermarkPrefixPath + key
	uploader := s3manager.NewUploader(sess)
	_, err2 := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(newKey),
		Body:   fb,
	})
	if err2 != nil {
		log.Fatalln("upload watermark to s3 error, "+newKey+" ;  ", err2)
	}
	log.Println("Upload success ! ", newKey)
	return newKey
}

/*
*
  - 下载aws s3中的文件
  - Download files in aws s3
  - for example
  - tmpPath := savePath + "/tmp-" + path.Base(key)
  - errDownload := downFileFromAwsS3(key, tmpPath)
  - if errDownload != nil {
  - log.Fatalln("download fail,key = "+key, errDownload)
  - }
*/
func downFileFromAwsS3(key string, tmpPath string) (err error) {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:      aws.String(awsRegion),
			Credentials: credentials.NewStaticCredentials(awsAk, awsSk, ""),
		}),
	)
	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024 // 10MB per part
		d.Concurrency = 4
	})
	file, err := os.Create(tmpPath)
	if err != nil {
		log.Fatalln("cant create download file source: "+tmpPath, err)
	}
	defer file.Close()
	numBytes, err2 := downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(key),
	})
	if err2 != nil {
		log.Fatalln("Unable to download item: "+tmpPath+" ;  ", err2)
	}
	log.Println("Downloaded success ! ", file.Name(), numBytes, "bytes")
	return nil
}

func getS3Key(urlString string, pathBase string) string {
	// 解析URL
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return pathBase
	}

	// 获取路径部分（去掉域名）
	path2 := parsedURL.Path
	if path2 == "" {
		path2 = parsedURL.EscapedPath()
	}
	path2 = strings.TrimLeft(path2, "/")
	return path2
}
