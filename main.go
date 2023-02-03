package main

import (
	"bytes"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	//"github.com/aws/aws-sdk-go-v2/aws"
	//"github.com/aws/aws-sdk-go-v2/credentials"
	//"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"os"
	"os/exec"
	"path"
)

//doc : https://docs.aws.amazon.com/zh_cn/lambda/latest/dg/golang-package.html#golang-package-libraries
//ffmpeg aws lambda layer 层： https://github.com/rpidanny/ffmpeg-lambda-layer

//水印在s3里面存放的前缀目录，比如以前的key=feed/sss.jpg ; 需要拼接为 key=watermark/feed/sss.jpg
var watermarkPrefixPath = "watermark/"

//图片下载和生成的存放目录
var savePath = "/tmp"

// RequestData 请求的json数据
type RequestData struct {
	Channel string `json:"channel"`
	Name    string `json:"name"`
	Key     string `json:"key"`
}

var (
	s3Bucket  = ""
	awsAk     = ""
	awsSk     = ""
	cdnDomain = "" //域名
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
		log.Println("你还没有配置S3的桶名")
		return
	}
	awsAk = os.Getenv("AwsAccessKey")
	if awsAk == "" {
		log.Println("你还没有aws ak")
		return
	}
	awsSk = os.Getenv("AwsSecretKey")
	if awsSk == "" {
		log.Println("你还没有aws sk")
		return
	}
	cdnDomain = os.Getenv("CdnDomain")
	//if cdnDomain == "" {
	//	log.Println("你还没有CdnDomain")
	//	return
	//}
	awsRegion = os.Getenv("AwsRegion")
	if awsRegion == "" {
		log.Println("你还没有配置aws region")
		return
	}
}

func main() {
	lambda.Start(HandleLambdaEvent)
}

func HandleLambdaEvent(event RequestData) (ResponseData, error) {
	//ffmpeg -i "https://cdn.google.live/video/75an099hrpb6od35elqopnceah-1670826684781993072384.mp4" -i "https://cdn.google.live/lambda/soundmate-logo%401x.png"  -filter_complex "overlay=10:10" 1.mp4
	key := event.Key
	//videoUrl := "https://cdn.google.live/video/75an099hrpb6od35elqopnceah-1670826684781993072384.mp4"
	//下载到临时文件夹
	log.Println("#####start video url=" + key)
	tmpPath := savePath + "/tmp-" + path.Base(key)
	errDownload := downFileFromAwsS3(key, tmpPath)
	if errDownload != nil {
		log.Fatalln("download fail,key = "+key, errDownload)
	}
	logoUrl := "logo.png"

	newSavePath := savePath + "/" + path.Base(key)
	cmd := exec.Command("ffmpeg", "-i", tmpPath, "-i", logoUrl, "-filter_complex", "overlay=10:10", newSavePath)
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
	log.Println("watermark file path : " + newSavePath)

	newKey := upLoadToAwsS3(newSavePath, key)
	return ResponseData{Body: BodyData{Data: newKey}}, nil
}

/**
 *  上传到s3
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

/**
 * 下载aws s3中的文件
 */
func downFileFromAwsS3(key string, tmpPath string) (err error) {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:      aws.String(awsRegion),
			Credentials: credentials.NewStaticCredentials(awsAk, awsSk, ""),
		}),
	)
	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024 // 64MB per part
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
