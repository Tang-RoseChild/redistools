package util

import (
	"encoding/json"
	"fmt"
	"reflect"

	log "github.com/Sirupsen/logrus"
	redigo "github.com/garyburd/redigo/redis"
)

var (
	RedisClient *redigo.Pool
)

var (
	RedisError          = fmt.Errorf("redis call failed")
	RedisNotExistsError = fmt.Errorf("redis key or id not exists")
)

func initRedis() {
	RedisClient = redigo.NewPool(
		func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", MatchingOptions.RedisAddr)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("SELECT", MatchingOptions.RedisDB); err != nil {
				panic(err)
			}
			return c, err
		},
		3,
	)
}

func HashGetObjectInfo(conn redigo.Conn, key, id string, obj interface{}) error {

	str, err := redigo.String(conn.Do("HGET", key, id))
	if err != nil {
		switch err {
		case redigo.ErrNil:
			return RedisNotExistsError
		default:
			return RedisError
		}
	}

	err = json.Unmarshal([]byte(str), obj)
	if err != nil {
		log.Print("mashal error : ", err)
		return err
	}
	return nil
}

// HashResetInfo reset info in redis hash . if info is nil, that means will delete the field of the given hash key.
// info  also need to be a pointer, 'cause like string,you can not use empty value to judge wether  need  to modify
// the value,and nil pointer means will not change that field of the struct
func HashResetObjectInfo(conn redigo.Conn, info interface{}, key, id string, pipeline ...bool) {

	isNil := (info == nil) || reflect.ValueOf(info).IsNil()
	if isNil {
		/// redis will delete the field or key
		if len(pipeline) > 0 {
			conn.Send("HDEL", key, id)
		} else {
			conn.Do("HDEL", key, id)
		}

		return
	}

	var rv reflect.Value
	if reflect.TypeOf(info).Kind() == reflect.Ptr {
		rv = reflect.ValueOf(info).Elem()
	} else {
		rv = reflect.ValueOf(info)
	}

	for i := 0; i < rv.NumField(); i++ {
		if rv.Field(i).Type().Kind() != reflect.Ptr {
			log.Printf(" need to be a pointer but get %v", rv.Field(i).Type().Kind())
			return
		}
	}

	str, err := json.Marshal(info)
	if err != nil {
		// log err & return
		log.Println("HashResetInfo Marshal error :: ", err)
		return
	}

	if len(pipeline) > 0 {
		conn.Send("EVAL", getHashResetObjScript(string(str), key, id), 0)
	} else {
		conn.Do("EVAL", getHashResetObjScript(string(str), key, id), 0)
	}

}

func getHashResetObjScript(resetStr, objKey, objId string) string {
	return fmt.Sprintf(resetObjScript, resetStr, objKey, objId)
}

const (
	resetObjScript = `
	--arg 1 : resetStr :json str
	--arg 2 : obj id
	local resetStr = '%s'
	local objKey = '%s'
	local objId = '%s'
	local resetObj =  cjson.decode(resetStr)

	local oldObj = {}
	local exists = redis.call("HEXISTS", objKey,objId)

	if exists > 0 then
		local oldObjStr = redis.call("HGET", objKey, objId )
		--print("oldObjStr " .. type(oldObjStr))
		oldObj = cjson.decode(oldObjStr)
	end

	for key, val in pairs(resetObj) do
		oldObj[key] = val
	end

	local newObjStr = cjson.encode(oldObj)
	--print(" newObjStr ::  " .. newObjStr)
	redis.call("HSET", objKey,objId,newObjStr)`
)

// GenPointer according to v,generate a new point of type v and value is v
// if v is a pointer, just return v.
func GenPointer(v interface{}) interface{} {
	if reflect.TypeOf(v).Kind() == reflect.Ptr {
		return v
	}

	rp := reflect.New(reflect.TypeOf(v))
	rp.Elem().Set(reflect.ValueOf(v))
	return rp.Interface()

}

// ConvertPointerFieldsToNormal convert point type to  normalType.
// normalType should be a pointer,which will set new vaule.
// normalType and pointType fields should have the same name
func ConvertPointerFieldsToNormal(pointType interface{}, normalType interface{}) {
	if pointType == nil || normalType == nil {
		log.Println("pointType or normalType shouldn't be nil")
		return
	}
	if reflect.TypeOf(normalType).Kind() != reflect.Ptr {
		log.Println("normalType should be a pointer")
		return
	}

	var rv reflect.Value
	if reflect.TypeOf(pointType).Kind() == reflect.Ptr {
		rv = reflect.ValueOf(pointType).Elem()
	} else {
		rv = reflect.ValueOf(pointType)
	}

	normalTypeRv := reflect.ValueOf(normalType).Elem()

	// check error first. otherwise,will change part of all fields
	for i := 0; i < normalTypeRv.NumField(); i++ {
		rvf := rv.FieldByName(normalTypeRv.Type().Field(i).Name)
		if !rvf.IsValid() {
			log.Printf("can't find  field(%s) in pointer interface{}", normalTypeRv.Type().Field(i).Name)
			return
		}
		if rvf.Type().Kind() != reflect.Ptr {
			log.Printf("pointType's field(%s)  type (%s) not a  pointer ", normalTypeRv.Type().Field(i).Name, rvf.Type().Name())
			return
		}

	}

	for i := 0; i < normalTypeRv.NumField(); i++ {
		rf := normalTypeRv.Field(i)
		rvf := rv.FieldByName(normalTypeRv.Type().Field(i).Name)

		if !rvf.IsNil() {
			rf.Set(rvf.Elem())
		}
	}

}
