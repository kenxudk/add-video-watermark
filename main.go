package main

import (
	"bytes"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"log"
	"os"
	"os/exec"
	"path"
)

//doc : https://docs.aws.amazon.com/zh_cn/lambda/latest/dg/golang-package.html#golang-package-libraries
//ffmpeg aws lambda layer 层： https://github.com/rpidanny/ffmpeg-lambda-layer

//水印在s3里面存放的前缀目录，比如以前的key=feed/sss.jpg ; 需要拼接为 key=watermark/feed/sss.jpg
//The prefix directory where the watermark is stored in s3, such as the previous key=feed/sss.jpg; It needs to be spliced as key=watermark/feed/sss.jpg
var watermarkPrefixPath = "watermark/"

//视频下载和生成的存放目录
//Storage directory for video download and generation
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
	cdnDomain = "" //域名 domain
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
	cdnDomain = os.Getenv("CdnDomain")
	//if cdnDomain == "" {
	//  你还没有CdnDomain
	//	log.Fatalln("You don't have Cdn Domain")
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
	//ffmpeg -i "https://cdn.google.live/video/75an099hrpb6od35elqopnceah-1670826684781993072384.mp4" -i "https://cdn.google.live/lambda/soundmate-logo%401x.png"  -filter_complex "overlay=10:10" 1.mp4
	key := event.Key
	//videoUrl := "https://cdn.google.live/video/75an099hrpb6od35elqopnceah-1670826684781993072384.mp4"
	//下载到临时文件夹(Download to temporary folder)
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

/**
 * 下载aws s3中的文件
 * Download files in aws s3
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
