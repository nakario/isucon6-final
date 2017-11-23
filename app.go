package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
	"net/http/pprof"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"goji.io"
	"goji.io/pat"
	"golang.org/x/net/context"
	"github.com/newrelic/go-agent"
)

var (
	sleeptime = 5 * time.Millisecond
	maxloop = 20
	dbx *sqlx.DB
	app newrelic.Application
)

type Token struct {
	ID        int64     `db:"id"`
	CSRFToken string    `db:"csrf_token"`
	CreatedAt time.Time `db:"created_at"`
}

type Point struct {
	ID       int64   `json:"id" db:"id"`
	StrokeID int64   `json:"stroke_id" db:"stroke_id"`
	X        float64 `json:"x" db:"x"`
	Y        float64 `json:"y" db:"y"`
}

type Stroke struct {
	ID        int64     `json:"id" db:"id"`
	RoomID    int64     `json:"room_id" db:"room_id"`
	Width     int       `json:"width" db:"width"`
	Red       int       `json:"red" db:"red"`
	Green     int       `json:"green" db:"green"`
	Blue      int       `json:"blue" db:"blue"`
	Alpha     float64   `json:"alpha" db:"alpha"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	Points    []Point   `json:"points" db:"points"`
}

type Room struct {
	ID           int64     `json:"id" db:"id"`
	Name         string    `json:"name" db:"name"`
	CanvasWidth  int       `json:"canvas_width" db:"canvas_width"`
	CanvasHeight int       `json:"canvas_height" db:"canvas_height"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	Strokes      []Stroke  `json:"strokes"`
	StrokeCount  int       `json:"stroke_count"`
	WatcherCount int       `json:"watcher_count"`
}

func startSQL(txn newrelic.Transaction, collection, operation string) newrelic.DatastoreSegment {
	return newrelic.DatastoreSegment{
		StartTime: txn.StartSegmentNow(),
		Product: newrelic.DatastoreMySQL,
		Collection: collection,
		Operation: operation,
	}
}

func printAndFlush(w http.ResponseWriter, content string) {
	fmt.Fprint(w, content)

	f, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)

		b, _ := json.Marshal(struct {
			Error string `json:"error"`
		}{Error: "Streaming unsupported!"})

		w.Write(b)
		fmt.Fprintln(os.Stderr, "Streaming unsupported!")
		return
	}
	f.Flush()
}

func checkToken(txn newrelic.Transaction, csrfToken string) (*Token, error) {
	if csrfToken == "" {
		return nil, nil
	}

	query := "SELECT `id`, `csrf_token`, `created_at` FROM `tokens`"
	query += " WHERE `csrf_token` = ? AND `created_at` > CURRENT_TIMESTAMP(6) - INTERVAL 1 DAY"

	t := &Token{}
	s := startSQL(txn, "tokens", "SELECT")
	err := dbx.Get(t, query, csrfToken)
	s.End()

	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}

	return t, nil
}

func getStrokePoints(txn newrelic.Transaction, strokeID int64) ([]Point, error) {
	query := "SELECT `id`, `stroke_id`, `x`, `y` FROM `points` WHERE `stroke_id` = ? ORDER BY `id` ASC"
	ps := []Point{}
	s := startSQL(txn, "points", "SELECT")
	err := dbx.Select(&ps, query, strokeID)
	s.End()
	if err != nil {
		return nil, err
	}
	return ps, nil
}

func getStrokes(txn newrelic.Transaction, roomID int64, greaterThanID int64) ([]Stroke, error) {
	query := "SELECT `id`, `room_id`, `width`, `red`, `green`, `blue`, `alpha`, `created_at` FROM `strokes`"
	query += " WHERE `room_id` = ? AND `id` > ? ORDER BY `id` ASC"
	strokes := []Stroke{}
	s := startSQL(txn, "strokes", "SELECT")
	err := dbx.Select(&strokes, query, roomID, greaterThanID)
	s.End()
	if err != nil {
		return nil, err
	}
	// 空スライスを入れてJSONでnullを返さないように
	for i := range strokes {
		strokes[i].Points = []Point{}
	}
	return strokes, nil
}

func getRoom(txn newrelic.Transaction, roomID int64) (*Room, error) {
	query := "SELECT `id`, `name`, `canvas_width`, `canvas_height`, `created_at` FROM `rooms` WHERE `id` = ?"
	r := &Room{}
	s := startSQL(txn, "rooms", "GET")
	err := dbx.Get(r, query, roomID)
	s.End()
	if err != nil {
		return nil, err
	}
	// 空スライスを入れてJSONでnullを返さないように
	r.Strokes = []Stroke{}
	return r, nil
}

func getWatcherCount(txn newrelic.Transaction, roomID int64) (int, error) {
	query := "SELECT COUNT(*) AS `watcher_count` FROM `room_watchers`"
	query += " WHERE `room_id` = ? AND `updated_at` > CURRENT_TIMESTAMP(6) - INTERVAL 3 SECOND"

	var watcherCount int
	s := startSQL(txn, "watcher_count", "SELECT")
	err := dbx.QueryRow(query, roomID).Scan(&watcherCount)
	s.End()
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return watcherCount, nil
}

func updateRoomWatcher(txn newrelic.Transaction, roomID int64, tokenID int64) error {
	query := "INSERT INTO `room_watchers` (`room_id`, `token_id`) VALUES (?, ?)"
	query += " ON DUPLICATE KEY UPDATE `updated_at` = CURRENT_TIMESTAMP(6)"

	s := startSQL(txn, "room_watchers", "INSERT")
	_, err := dbx.Exec(query, roomID, tokenID)
	s.End()
	return err
}

func outputErrorMsg(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")

	b, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: msg})

	w.WriteHeader(status)
	w.Write(b)
}

func outputError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(http.StatusInternalServerError)

	b, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: "InternalServerError"})

	w.Write(b)
	fmt.Fprintln(os.Stderr, err.Error())
}

func postAPICsrfToken(w http.ResponseWriter, r *http.Request) {
	txn := app.StartTransaction("postAPICsrfToken", w, r)
	defer txn.End()
	query := "INSERT INTO `tokens` (`csrf_token`) VALUES"
	query += " (SHA2(CONCAT(RAND(), UUID_SHORT()), 256))"

	s := startSQL(txn, "tokens", "INSERT")
	result, err := dbx.Exec(query)
	s.End()
	if err != nil {
		outputError(w, err)
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		outputError(w, err)
		return
	}

	t := Token{}
	query = "SELECT `id`, `csrf_token`, `created_at` FROM `tokens` WHERE id = ?"
	s2 := startSQL(txn, "tokens", "SELECT")
	err = dbx.Get(&t, query, id)
	s2.End()
	if err != nil {
		outputError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json;charset=utf-8")

	b, _ := json.Marshal(struct {
		Token string `json:"token"`
	}{Token: t.CSRFToken})

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getAPIRooms(w http.ResponseWriter, r *http.Request) {
	txn := app.StartTransaction("getAPIRooms", w, r)
	defer txn.End()
	query := "SELECT `room_id`, MAX(`id`) AS `max_id` FROM `strokes`"
	query += " GROUP BY `room_id` ORDER BY `max_id` DESC LIMIT 100"

	type result struct {
		RoomID int64 `db:"room_id"`
		MaxID  int64 `db:"max_id"`
	}

	results := []result{}

	s := startSQL(txn, "strokes", "SELECT")
	err := dbx.Select(&results, query)
	s.End()
	if err != nil {
		outputError(w, err)
		return
	}

	rooms := []*Room{}

	for _, r := range results {
		room, err := getRoom(txn, r.RoomID)
		if err != nil {
			outputError(w, err)
			return
		}
		s, err := getStrokes(txn, room.ID, 0)
		if err != nil {
			outputError(w, err)
			return
		}
		room.StrokeCount = len(s)
		rooms = append(rooms, room)
	}

	w.Header().Set("Content-Type", "application/json;charset=utf-8")

	b, _ := json.Marshal(struct {
		Rooms []*Room `json:"rooms"`
	}{Rooms: rooms})

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func postAPIRooms(w http.ResponseWriter, r *http.Request) {
	txn := app.StartTransaction("postAPIRooms", w, r)
	defer txn.End()
	t, err := checkToken(txn, r.Header.Get("x-csrf-token"))

	if err != nil {
		outputError(w, err)
		return
	}

	if t == nil {
		outputErrorMsg(w, http.StatusBadRequest, "トークンエラー。ページを再読み込みしてください。")
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		outputError(w, err)
		return
	}

	postedRoom := Room{}
	err = json.Unmarshal(body, &postedRoom)
	if err != nil {
		outputError(w, err)
		return
	}

	if postedRoom.Name == "" || postedRoom.CanvasWidth == 0 || postedRoom.CanvasHeight == 0 {
		outputErrorMsg(w, http.StatusBadRequest, "リクエストが正しくありません。")
		return
	}

	s := startSQL(txn, "rooms", "INSERT")
	tx := dbx.MustBegin()
	query := "INSERT INTO `rooms` (`name`, `canvas_width`, `canvas_height`)"
	query += " VALUES (?, ?, ?)"

	result := tx.MustExec(query, postedRoom.Name, postedRoom.CanvasWidth, postedRoom.CanvasHeight)
	roomID, err := result.LastInsertId()
	if err != nil {
		outputError(w, err)
		return
	}

	query = "INSERT INTO `room_owners` (`room_id`, `token_id`) VALUES (?, ?)"
	tx.MustExec(query, roomID, t.ID)

	err = tx.Commit()
	s.End()
	if err != nil {
		tx.Rollback()
		outputError(w, err)
		return
	}

	room, err := getRoom(txn, roomID)
	if err != nil {
		outputError(w, err)
		return
	}

	b, _ := json.Marshal(struct {
		Room *Room `json:"room"`
	}{Room: room})

	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getAPIRoomsID(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	txn := app.StartTransaction("getAPIRoomsID", w, r)
	defer txn.End()
	idStr := pat.Param(ctx, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		outputErrorMsg(w, http.StatusNotFound, "この部屋は存在しません。")
		return
	}

	room, err := getRoom(txn, id)
	if err != nil {
		if err == sql.ErrNoRows {
			outputErrorMsg(w, http.StatusNotFound, "この部屋は存在しません。")
		} else {
			outputError(w, err)
		}
		return
	}

	strokes, err := getStrokes(txn, room.ID, 0)
	if err != nil {
		outputError(w, err)
		return
	}

	for i, s := range strokes {
		p, err := getStrokePoints(txn, s.ID)
		if err != nil {
			outputError(w, err)
			return
		}
		strokes[i].Points = p
	}

	room.Strokes = strokes
	room.WatcherCount, err = getWatcherCount(txn, room.ID)
	if err != nil {
		outputError(w, err)
		return
	}

	b, _ := json.Marshal(struct {
		Room *Room `json:"room"`
	}{Room: room})

	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func getAPIStreamRoomsID(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	txn := app.StartTransaction("getAPIStreamRoomsID", w, r)
	defer txn.End()
	w.Header().Set("Content-Type", "text/event-stream")

	idStr := pat.Param(ctx, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return
	}

	t, err := checkToken(txn, r.URL.Query().Get("csrf_token"))

	if err != nil {
		outputError(w, err)
		return
	}
	if t == nil {
		printAndFlush(w, "event:bad_request\n"+"data:トークンエラー。ページを再読み込みしてください。\n\n")
		return
	}

	room, err := getRoom(txn, id)
	if err != nil {
		if err == sql.ErrNoRows {
			printAndFlush(w, "event:bad_request\n"+"data:この部屋は存在しません\n\n")
		} else {
			outputError(w, err)
		}
		return
	}

	err = updateRoomWatcher(txn, room.ID, t.ID)
	if err != nil {
		outputError(w, err)
		return
	}

	watcherCount, err := getWatcherCount(txn, room.ID)
	if err != nil {
		outputError(w, err)
		return
	}

	printAndFlush(w, "retry:500\n\n"+"event:watcher_count\n"+"data:"+strconv.Itoa(watcherCount)+"\n\n")

	var lastStrokeID int64
	lastEventIDStr := r.Header.Get("Last-Event-ID")
	if lastEventIDStr != "" {
		lastEventID, err := strconv.ParseInt(lastEventIDStr, 10, 64)
		if err != nil {
			outputError(w, err)
			return
		}
		lastStrokeID = lastEventID
	}

	loop := maxloop
	for loop > 0 {
		loop--
		time.Sleep(sleeptime)

		strokes, err := getStrokes(txn, room.ID, int64(lastStrokeID))
		if err != nil {
			outputError(w, err)
			return
		}

		for _, s := range strokes {
			s.Points, err = getStrokePoints(txn, s.ID)
			if err != nil {
				outputError(w, err)
				return
			}
			d, _ := json.Marshal(s)
			printAndFlush(w, "id:"+strconv.FormatInt(s.ID, 10)+"\n\n"+"event:stroke\n"+"data:"+string(d)+"\n\n")
			lastStrokeID = s.ID
		}

		err = updateRoomWatcher(txn, room.ID, t.ID)
		if err != nil {
			outputError(w, err)
			return
		}

		newWatcherCount, err := getWatcherCount(txn, room.ID)
		if err != nil {
			outputError(w, err)
			return
		}
		if newWatcherCount != watcherCount {
			watcherCount = newWatcherCount
			printAndFlush(w, "event:watcher_count\n"+"data:"+strconv.Itoa(watcherCount)+"\n\n")
		}
	}
}

func postAPIStrokesRoomsID(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	txn := app.StartTransaction("postAPIStrokesRoomsID", w, r)
	defer txn.End()
	t, err := checkToken(txn, r.Header.Get("x-csrf-token"))

	if err != nil {
		outputError(w, err)
		return
	}
	if t == nil {
		outputErrorMsg(w, http.StatusBadRequest, "トークンエラー。ページを再読み込みしてください。")
		return
	}

	idStr := pat.Param(ctx, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		outputErrorMsg(w, http.StatusNotFound, "この部屋は存在しません。")
		return
	}

	room, err := getRoom(txn, id)
	if err != nil {
		if err == sql.ErrNoRows {
			outputErrorMsg(w, http.StatusNotFound, "この部屋は存在しません。")
		} else {
			outputError(w, err)
		}
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		outputError(w, err)
		return
	}
	postedStroke := Stroke{}
	err = json.Unmarshal(body, &postedStroke)
	if err != nil {
		outputError(w, err)
		return
	}

	if postedStroke.Width == 0 || len(postedStroke.Points) == 0 {
		outputErrorMsg(w, http.StatusBadRequest, "リクエストが正しくありません。")
		return
	}

	strokes, err := getStrokes(txn, room.ID, 0)
	if err != nil {
		outputError(w, err)
		return
	}
	if len(strokes) == 0 {
		query := "SELECT COUNT(*) AS cnt FROM `room_owners` WHERE `room_id` = ? AND `token_id` = ?"
		cnt := 0
		s := startSQL(txn, "room_owners", "SELECT")
		err = dbx.QueryRow(query, room.ID, t.ID).Scan(&cnt)
		s.End()
		if err != nil {
			outputError(w, err)
			return
		}
		if cnt == 0 {
			outputErrorMsg(w, http.StatusBadRequest, "他人の作成した部屋に1画目を描くことはできません")
			return
		}
	}

	seg := startSQL(txn, "strokes, points", "INSERT, SELECT")
	tx := dbx.MustBegin()
	query := "INSERT INTO `strokes` (`room_id`, `width`, `red`, `green`, `blue`, `alpha`)"
	query += " VALUES(?, ?, ?, ?, ?, ?)"

	result := tx.MustExec(query,
		room.ID,
		postedStroke.Width,
		postedStroke.Red,
		postedStroke.Green,
		postedStroke.Blue,
		postedStroke.Alpha,
	)
	strokeID, err := result.LastInsertId()
	if err != nil {
		outputError(w, err)
		return
	}

	query = "INSERT INTO `points` (`stroke_id`, `x`, `y`) VALUES (?, ?, ?)"
	for _, p := range postedStroke.Points {
		tx.MustExec(query, strokeID, p.X, p.Y)
	}

	err = tx.Commit()
	seg.End()
	if err != nil {
		tx.Rollback()
		outputError(w, err)
		return
	}

	query = "SELECT `id`, `room_id`, `width`, `red`, `green`, `blue`, `alpha`, `created_at` FROM `strokes`"
	query += " WHERE `id` = ?"
	s := Stroke{}
	s2 := startSQL(txn, "strokes", "SELECT")
	err = dbx.Get(&s, query, strokeID)
	s2.End()
	if err != nil {
		outputError(w, err)
		return
	}

	s.Points, err = getStrokePoints(txn, strokeID)
	if err != nil {
		outputError(w, err)
		return
	}

	b, _ := json.Marshal(struct {
		Stroke Stroke `json:"stroke"`
	}{Stroke: s})

	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func main() {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MYSQL_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		log.Fatalf("Failed to read DB port number from an environment variable MYSQL_PORT.\nError: %s", err.Error())
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("MYSQL_PASS")
	dbname := "isuketch"

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		user,
		password,
		host,
		port,
		dbname,
	)

	dbx, err = sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s.", err.Error())
	}
	defer dbx.Close()

	config := newrelic.NewConfig("ISUketch", os.Getenv("NEW_RELIC_KEY"))
	app, err = newrelic.NewApplication(config)

	if err != nil {
		log.Fatalf("Failed to connect to New Relic:", err)
	}

	mux := goji.NewMux()
	mux.HandleFunc(pat.Post("/api/csrf_token"), postAPICsrfToken)
	mux.HandleFunc(pat.Get("/api/rooms"), getAPIRooms)
	mux.HandleFunc(pat.Post("/api/rooms"), postAPIRooms)
	mux.HandleFuncC(pat.Get("/api/rooms/:id"), getAPIRoomsID)
	mux.HandleFuncC(pat.Get("/api/stream/rooms/:id"), getAPIStreamRoomsID)
	mux.HandleFuncC(pat.Post("/api/strokes/rooms/:id"), postAPIStrokesRoomsID)

	mux.Handle(pat.Get("/debug/pprof/"), http.HandlerFunc(pprof.Index))
	mux.Handle(pat.Get("/debug/pprof/cmdline"), http.HandlerFunc(pprof.Cmdline))
	mux.Handle(pat.Get("/debug/pprof/profile"), http.HandlerFunc(pprof.Profile))
	mux.Handle(pat.Get("/debug/pprof/symbol"), http.HandlerFunc(pprof.Symbol))

	log.Fatal(http.ListenAndServe("0.0.0.0:80", mux))
}
