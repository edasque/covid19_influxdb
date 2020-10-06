package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	// influxdb2 "github.com/influxdata/influxdb-client-go"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
)

// API for ANSI-COLOR output.
// var aur aurora.Aurora

type covidtrackingV1 []struct {
	Date                     int    `json:"date"`
	State                    string `json:"state"`
	Positive                 int    `json:"positive"`
	Negative                 int    `json:"negative"`
	Pending                  int    `json:"pending"`
	HospitalizedCurrently    int    `json:"hospitalizedCurrently"`
	HospitalizedCumulative   int    `json:"hospitalizedCumulative"`
	InIcuCurrently           int    `json:"inIcuCurrently"`
	InIcuCumulative          int    `json:"inIcuCumulative"`
	OnVentilatorCurrently    int    `json:"onVentilatorCurrently"`
	OnVentilatorCumulative   int    `json:"onVentilatorCumulative"`
	Recovered                int    `json:"recovered"`
	Hash                     string `json:"hash"`
	DateChecked              string `json:"dateChecked"`
	Death                    int    `json:"death"`
	Hospitalized             int    `json:"hospitalized"`
	Total                    int    `json:"total"`
	TotalTestResults         int    `json:"totalTestResults"`
	PosNeg                   int    `json:"posNeg"`
	Fips                     string `json:"fips"`
	DeathIncrease            int    `json:"deathIncrease"`
	HospitalizedIncrease     int    `json:"hospitalizedIncrease"`
	NegativeIncrease         int    `json:"negativeIncrease"`
	PositiveIncrease         int    `json:"positiveIncrease"`
	TotalTestResultsIncrease int    `json:"totalTestResultsIncrease"`
}

// main COVID data in JSON format for all states
const statesAPIURL = "https://api.covidtracking.com/v1/states/daily.json"

//RtCSVURL: CSV file for Rt
const rtCSVURL = "https://d14wlfuexuxgcm.cloudfront.net/covid/rt.csv"

// Config : structure for config file. Have a file called config.json in the current directory, see config.json.sample for a referenvce
type Config struct {
	Influxdbhost     string `json:"influxdbhost"`
	Influxdbport     string `json:"influxdbport"`
	Influxdbdatabase string `json:"influxdbdatabase"`
	Influxdbusername string `json:"influxdbusername"`
	Influxdbpassword string `json:"influxdbpassword"`
}

type covidtrackingRt struct {
	date                    time.Time
	region                  string
	index                   string
	mean                    float64
	median                  string
	lower80                 string
	upper80                 string
	infections              string
	testAdjustedPositive    string
	testAdjustedPositiveRaw string
	positive                string
	tests                   string
	newTests                string
	newCases                string
	newDeaths               string
}

var config Config

func makeRtAPICall() ([][]string, error) {
	req, newRequestError := http.NewRequest("GET", rtCSVURL, nil)

	if newRequestError != nil {
		fmt.Fprintf(os.Stderr, "Error getting request: %v\n", newRequestError)
		fmt.Fprint(os.Stderr, "URL:", rtCSVURL)
		//TODO do something if returning nil
		// return nil // , err

		return nil, newRequestError
	}

	// start := time.Now()
	res, httperror := http.DefaultClient.Do(req)
	// duration := time.Since(start)

	if httperror != nil {
		fmt.Fprintf(os.Stderr, "HTTP error: %v for url: %v\n", httperror, statesAPIURL)

		//TODO do something if returning nil
		// return nil // , err

		return nil, httperror

	}

	defer res.Body.Close()

	influxDBcnx, influxdbcnxerror := connectToInfluxDB()
	defer influxDBcnx.Close()

	if influxdbcnxerror != nil {
		fmt.Println("Couldn't connect to InfluxDB:")
		log.Fatalln(influxdbcnxerror)

	}

	// body, _ := ioutil.ReadAll(res.Body)
	// r := bytes.NewReader(byteData)

	lines, err := csv.NewReader(res.Body).ReadAll()
	if err != nil {
		return [][]string{}, err
	}
	layout := "2006-01-02"

	numberOfRecords := len(lines)

	for index, line := range lines {
		if index != 0 {

			t, err := time.Parse(layout, line[0])
			now := time.Now().Format("1/2/2006 15:04:05 -0700")

			doNotInsertBefore := time.Now().Add(time.Hour * 24 * -7)

			if t.Before(doNotInsertBefore) {

				fmt.Printf("%v - Will not insert %v/%v - Data for %v (%v): RtMean=%v\n", now, index+1, numberOfRecords, line[1], t, line[3])

			} else {

				mean, floaterr := strconv.ParseFloat(line[3], 64)
				if err == nil && floaterr == nil {

					data := covidtrackingRt{
						date:   t,
						region: line[1],
						mean:   mean,
					}

					tags := map[string]string{
						"state": data.region}

					fields := map[string]interface{}{
						"RtMean": mean,
					}

					// Add a parameter for running without inserting
					if true {
						writeToInfluxDB(influxDBcnx, "covid19Rt", tags, fields, t)
						fmt.Printf("%v - Inserted %v/%v - Data for %v (%v): RtMean=%v\n", now, index+1, numberOfRecords, data.region, data.date, strconv.FormatFloat(data.mean, 'f', -1, 64))

					}

				}
			}

		}
	}

	return lines, nil
}

func makeAPICall() (*covidtrackingV1, error) {
	req, newRequestError := http.NewRequest("GET", statesAPIURL, nil)

	if newRequestError != nil {
		fmt.Fprintf(os.Stderr, "Error getting request: %v\n", newRequestError)
		fmt.Fprint(os.Stderr, "URL:", statesAPIURL)
		//TODO do something if returning nil
		// return nil // , err

		return nil, newRequestError
	}

	// start := time.Now()
	res, httperror := http.DefaultClient.Do(req)
	// duration := time.Since(start)

	if httperror != nil {
		fmt.Fprintf(os.Stderr, "HTTP error: %v for url: %v\n", httperror, statesAPIURL)

		//TODO do something if returning nil
		// return nil // , err

		return nil, httperror

	}

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	covidStateResponse := covidtrackingV1{}

	// fmt.Println(string(body))

	unmarshalError := json.Unmarshal(body, &covidStateResponse)
	if unmarshalError != nil {
		fmt.Fprintf(os.Stderr, "covidTracking response Unmarshalling error: %v\n", unmarshalError)
		fmt.Fprintf(os.Stderr, string(body), "\n")

		return nil, unmarshalError
	}
	return &covidStateResponse, nil
}

func connectToInfluxDB() (client.Client, error) {

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://" + config.Influxdbhost + ":" + config.Influxdbport,
		Username: config.Influxdbusername,
		Password: config.Influxdbpassword,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}

	return c, err
}

func writeToInfluxDB(c client.Client, metricName string, tags map[string]string,
	fields map[string]interface{}, t time.Time) {

	bp, nBPError := client.NewBatchPoints(client.BatchPointsConfig{
		Database: config.Influxdbdatabase,
	})

	if nBPError != nil {
		fmt.Println("Error creating Batchpoints with config: ", nBPError)

	}

	// 	fmt.Println(bp)

	p, newPointError := client.NewPoint(metricName, tags, fields, t)

	if newPointError != nil {
		fmt.Println("Error creating Batchpoints with config: ", newPointError)

	}

	bp.AddPoint(p)

	// fmt.Println(tags)
	// fmt.Println(fields)
	// fmt.Println("-------------------")

	writeErr := c.Write(bp)
	if writeErr != nil {
		fmt.Printf("InfluxDB write error: %v", writeErr)
	}

}
func ingestResponse(response covidtrackingV1) {

	influxDBcnx, influxdbcnxerror := connectToInfluxDB()
	defer influxDBcnx.Close()

	if influxdbcnxerror != nil {
		fmt.Println("Couldn't connect to InfluxDB:")
		log.Fatalln(influxdbcnxerror)

	}

	// _, err = connect.Exec(`
	// 	CREATE TABLE IF NOT EXISTS covid.DailyStatsByState (
	// 	date DateTime,
	// 	state FixedString(2),
	// 	death UInt32,
	// 	deathIncrease UInt32,
	// 	hospitalizedCumulative UInt32,
	// 	hospitalized UInt32,
	// 	hospitalizedIncrease UInt32
	// 	) ENGINE = Memory

	// `)
	numberOfRecords := len(response)

	for index, element := range response {
		// index is the index where we are
		// element is the element from someSlice for where we are

		// fmt.Print(element.Date)
		// fmt.Print(element.State)
		// fmt.Println(element.Death)

		layout := "20060102"
		t, err := time.Parse(layout, strconv.Itoa(int(element.Date)))

		// format is Mon Jan 2 15:04:05 -0700 MST 2006
		now := time.Now().Format("1/2/2006 15:04:05 -0700")
		tformatted := t.Format("1/2/2006 15:04:05 -0700")

		//make this a parameter, now set to 7d
		doNotInsertBefore := time.Now().Add(time.Hour * 24 * -7)

		if t.Before(doNotInsertBefore) {

			fmt.Printf("%v - Will not insert %v/%v - Data for %v - %v (D:%v,C:%v,D+:%v\n", now, index+1, numberOfRecords, tformatted, element.State, element.Death, element.Positive, element.DeathIncrease)

		} else {

			if err != nil {
				fmt.Println(err)
			}

			tags := map[string]string{
				"state": element.State}
			fields := map[string]interface{}{
				"case":                   element.Positive,
				"hospitalizedCurrently":  element.HospitalizedCurrently,
				"hospitalizedCumulative": element.HospitalizedCumulative,
				"hospitalizedIncrease":   element.HospitalizedIncrease,

				"onVentilatorCurrently":  element.OnVentilatorCurrently,
				"onVentilatorCumulative": element.OnVentilatorCumulative,

				"inIcuCurrently":  element.InIcuCurrently,
				"inIcuCumulative": element.InIcuCumulative,

				"death":                    element.Death,
				"deathIncrease":            element.DeathIncrease,
				"recovered":                element.Recovered,
				"totalTestResultsIncrease": element.TotalTestResultsIncrease,
				"positiveIncrease":         element.PositiveIncrease,
				"negativeIncrease":         element.NegativeIncrease,
			}
			// if element.State == "MA" {
			// 	fmt.Println(element)
			// 	fmt.Println(tags)
			// 	fmt.Println(t)
			// 	fmt.Println(fields)
			// 	fmt.Println("-------------------")
			// }

			// Add a parameter for running without inserting
			if true {
				writeToInfluxDB(influxDBcnx, "covid19", tags, fields, t)
			}
			fmt.Printf("%v - Inserted %v/%v - Data for %v - %v (D:%v,C:%v,D+:%v\n", now, index+1, numberOfRecords, tformatted, element.State, element.Death, element.Positive, element.DeathIncrease)
		}

	}

}

func main() {

	file, e := ioutil.ReadFile("./config.json")
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	config = Config{}
	configUnMarshalError := json.Unmarshal(file, &config)

	if configUnMarshalError != nil {
		fmt.Printf("File config error: %v\n", configUnMarshalError)
		os.Exit(1)

	}

	covidStateResponse, err := makeAPICall()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)

	}

	numberOfRecords := len(*covidStateResponse)

	fmt.Printf("Number of records retrieved: %v\n", numberOfRecords)

	// ingestResponse(*covidStateResponse)
	makeRtAPICall()
	// fmt.Printf("turlupin: %v\n", turlupin)

}
