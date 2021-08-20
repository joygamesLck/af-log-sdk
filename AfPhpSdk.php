<?php
/**
 * Date: 2018/8/2
 * Time: 17:14
 */
//define('SDK_VERSION', '1.0.0');

/**
 * 数据格式错误异常
 */
class AfDataException extends \Exception
{
}

/**
 * 网络异常
 */
class AfDataNetWorkException extends \Exception
{
}

class AfDataAnalytics
{
    private $consumer;
    private $publicProperties;
    private $enableUUID;

    // what相关字段
    private  $whatKeys = [
        'event','event_type','project','log_level','sub_event','sub_event_desc','event_content'
    ];
    // 共用相关字段
    private  $commonKeys = [
        'app_id','channel_id','sub_channel_id','ad_id'
    ];
    // who相关字段
    private  $whoKeys = [
        'user_id','account_id','distinct_id'
    ];
    // when相关字段
    private  $whenKeys = [
        'event_time','ymd','ym','year','month','week','day','hour','minute','server_time','ym','zone_offset'
    ];
    // where相关字段
    private  $whereKeys = ['ip'];
    // how相关字段
    private  $howKeys = [
        'brand','manufacturer','model','os','os_version','screen_height','screen_width','screen_orientation','wifi','carrier','network_type','device_id','system_language','idfa','imei','android_id','is_emulator','supported_abis'
    ];
    // cp客户端相关字段
    private  $cpClientKeys = [
        'app_name','app_version','bundle_id','game_name'
    ];
    // sdk客户端相关字段
    private  $sdkClientKeys = [
        'joy_sdk_version','channel_sdk_version'
    ];
    // sdk日志相关字段
    private  $sdkLogKeys = [
        'lib','lib_version','lib_method','lib_detail'
    ];
    private  $pKeys = ['role_level','event_unique_id','cp_order_id', 'order_id', 'pay_channel', 'role_name', 'role_id', 'username'];

    private $allKeys;

    function __construct($consumer, $enableUUID = false)
    {
        $this->consumer = $consumer;
        $this->enableUUID = $enableUUID;
        $this->clear_public_properties();
        $this->merge_properties();
    }

    /**
     * 设置用户属性, 覆盖之前设置的属性.
     * @param array $properties 用户属性
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function user_set($properties = array())
    {
        return $this->add('user_set', null, $properties);
    }

    /**
     * 设置用户属性, 如果属性已经存在, 则操作无效.
     * @param array $properties 用户属性
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function user_setOnce($properties = array())
    {
        return $this->add('user_setOnce', null, $properties);
    }

    /**
     * 修改数值类型的用户属性.
     * @param array $properties 用户属性, 其值需为 Number 类型
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function user_add($properties = array())
    {
        return $this->add('user_add', null, $properties);
    }

    /**
     * 追加一个用户的某一个或者多个集合
     * @param array $properties key上传的是非关联数组
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function user_append($properties = array())
    {
        return $this->add('user_append', null, $properties);
    }

    /**
     * 删除用户属性
     * @param string $distinct_id 访客 ID
     * @param string $account_id 账户 ID
     * @param array $properties key上传的是删除的用户属性
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function user_unset($distinct_id, $account_id, $properties = array())
    {
        if (is_null($properties)) {
            throw new AfDataException("property cannot be empty .");
        }
        $arr = array_fill_keys($properties, 0);
        return $this->add($distinct_id, $account_id, 'user_unset', null, null, $arr);
    }

    /**
     * 删除用户, 此操作不可逆, 请谨慎使用.
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function user_del()
    {
        return $this->add('user_del', null, array());
    }

    /**
     * 上报事件.
     * @param array $properties 事件属性
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function track($properties = array())
    {
        return $this->add( 'track', null, $properties);
    }

    /**
     * 上报事件.
     * @param string $event_id 事件ID
     * @param array $properties 事件属性
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function track_update($event_id, $properties = array())
    {
        return $this->add('track_update', $event_id, $properties);
    }

    /**
     * 上报事件.
     * @param string $event_id 事件ID
     * @param array $properties 事件属性
     * @return boolean
     * @throws Exception 数据传输，或者写文件失败
     */
    public function track_overwrite( $event_id, $properties = array())
    {
        return $this->add('track_overwrite', $event_id, $properties);
    }

    private function add($type, $event_id, $properties)
    {
        $event = array();
        if (isset($properties['event']) && !is_null($properties['event']) && !is_string($properties['event'])) {
            throw new AfDataException("event name must be a str.");
        }
        if (isset($properties['sub_event']) && !is_null($properties['sub_event']) && !is_string($properties['sub_event'])) {
            throw new AfDataException("sub_event name must be a str.");
        }
        if ($type == 'track') {
            $properties = array_merge($properties, $this->publicProperties);
            if (array_key_exists('#first_check_id', $properties)) {
                $event['#first_check_id'] = $properties['#first_check_id'];
                unset($properties['#first_check_id']);
            }
        }
        if ($type == 'track_update' || $type == 'track_overwrite') {
            $properties = array_merge($properties, $this->publicProperties);
            $event['#event_id'] = $event_id;
        }
        $event['#event_type'] = $type;

        $event = array_merge($event, $this->extractUserTime($properties));
        $event['#p'] = $this->extractPData($properties);

        foreach ($this->allKeys as $key){
            $eventKey = '#' . $key;
            if(!array_key_exists($eventKey, $event)){
                $event[$eventKey] = '';
            }
            if(array_key_exists($key, $properties)){
                if(empty($event[$eventKey])){
                    $event[$eventKey] = $properties[$key];
                }
                unset($properties[$key]);
            }
        }
        //检查properties
        $properties = $this->assertProperties($type, $properties);
        if (count($properties) > 0) {
            $event['#event_content'] = $properties;
        }

        return $this->consumer->send(json_encode($event));
    }

    private function assertProperties($type, $properties)
    {
        // 检查 properties
        if (is_array($properties)) {
            $name_pattern = "/^(#|[a-z])[a-z0-9_]{0,49}$/i";
            if (!$properties) {
                return;
            }
            foreach ($properties as $key => &$value) {
                if (is_null($value)) {
                    continue;
                }
                if (!is_string($key)) {
                    throw new AfDataException("property key must be a str. [key=$key]");
                }
                if (strlen($key) > 50) {
                    throw new AfDataException("the max length of property key is 50. [key=$key]");
                }
                if (!preg_match($name_pattern, $key)) {
                    throw new AfDataException("property key must be a valid variable name. [key='$key']]");
                }
                if (!is_scalar($value) && !$value instanceof DateTime && !is_array($value)) {
                    throw new AfDataException("property value must be a str/int/float/datetime/array. [key='$key']");
                }
                if ($type == 'user_add' && !is_numeric($value)) {
                    throw new AfDataException("Type user_add only support Number [key='$key']");
                }
                // 如果是 DateTime，Format 成字符串
                if ($value instanceof DateTime) {
                    $properties[$key] = $this->getFormatDate($value->getTimestamp());
                }
                //如果是数组
                if (is_array($value)) {
                    if (array_values($value) !== $value) {
                        throw new AfDataException("[array] property must not be associative. [key='$key']");
                    }
                    for ($i = 0; $i < count($value); $i++) {
                        if ($value[$i] instanceof DateTime) {
                            $value[$i] = $this->getFormatDate($value[$i]->getTimestamp());
                        }
                    }
                }
            }
        } else {
            throw new AfDataException("property must be an array.");
        }
        return $properties;
    }

    public function getDatetime()
    {
        return $this->getFormatDate(null, 'Y-m-d H:i:s.u');
    }

    function getFormatDate($time = null, $format = 'Y-m-d H:i:s.u')
    {
        $utimestamp = microtime(true);
        $timestamp = floor($utimestamp);
        $milliseconds = round(($utimestamp - $timestamp) * 1000);
        if ($milliseconds == 1000) {
            $timestamp = strtotime("+1second", $timestamp);
            $milliseconds = 0;
        }
        $new_format = preg_replace('`(?<!\\\\)u`', sprintf("%03d", $milliseconds), $format);
        if ($time !== null) {
            return date($new_format, $time);
        }
        return date($new_format, $timestamp);
    }

    private function extractPData(&$properties = array())
    {
        $p = [];
        foreach ($this->pKeys as $key){
            if(array_key_exists($key, $properties)){
                $p[$key] = $properties[$key];
                unset($properties[$key]);
            }
        }
        return $p;
    }

    private function extractUserTime(&$properties = array())
    {
        $time = [];
        if (array_key_exists('event_time', $properties)) {
            $time['#event_time'] = $properties['event_time'];
            $timestamp = strtotime(explode('.', $time['#event_time'])[0]);
            $time['#ymd'] = intval(date('Ymd', $timestamp));
            $time['#ym'] = intval(date('Ym', $timestamp));
            $time['#year'] = intval(date('Y', $timestamp));
            $time['#month'] = intval(date('m', $timestamp));
            $time['#week'] = intval(date('w', $timestamp)) == 0 ? 7 : intval(date('w', $timestamp));
            $time['#day'] = intval(date('d', $timestamp));
            $time['#hour'] = intval(date('H', $timestamp));
            $time['#minute'] = intval(date('i', $timestamp));
            unset($properties['event_time']);
        }
        return $time;
    }

    private function extractStringProperty($key, &$properties = array())
    {
        if (array_key_exists($key, $properties)) {
            $value = $properties[$key];
            unset($properties[$key]);
            return $value;
        }
        return '';
    }

    function uuid()
    {
        $chars = md5(uniqid(mt_rand(), true));
        $uuid = substr($chars, 0, 8) . '-'
            . substr($chars, 8, 4) . '-'
            . substr($chars, 12, 4) . '-'
            . substr($chars, 16, 4) . '-'
            . substr($chars, 20, 12);
        return $uuid;
    }

    /**
     * 清空公共属性
     */
    public function clear_public_properties()
    {
        $this->publicProperties = array(
            'lib'         => 'php',
            'lib_version' => '1.0.0',
        );
    }

    /**
     * 合并属性key
     */
    public function merge_properties()
    {
        $this->allKeys = array_merge($this->whatKeys, $this->whenKeys, $this->whereKeys, $this->whoKeys, $this->howKeys, $this->commonKeys, $this->cpClientKeys, $this->sdkClientKeys, $this->sdkLogKeys);
    }

    /**
     * 设置每个事件都带有的一些公共属性
     *
     * @param array $super_properties 公共属性
     */
    public function register_public_properties($super_properties)
    {
        $this->publicProperties = array_merge($this->publicProperties, $super_properties);
    }

    /**
     * 立即刷新
     */
    public function flush()
    {
        $this->consumer->flush();
    }

    /**
     * 关闭 sdk 接口
     */
    public function close()
    {
        $this->consumer->close();
    }

}

abstract class AfAbstractConsumer
{
    /**
     * 发送一条消息, 返回true为send成功。
     * @param string $message 发送的消息体
     * @return bool
     */
    public abstract function send($message);

    /**
     * 立即发送所有未发出的数据。
     * @return bool
     */
    public function flush()
    {
        return true;
    }

    /**
     * 关闭 Consumer 并释放资源。
     * @return bool
     */
    public abstract function close();
}

/**
 * 批量实时写本地文件，文件以天为分隔，需要与 LogBus 搭配使用进行数据上传. 建议使用，不支持多线程
 */
class AfFileConsumer extends AfAbstractConsumer
{
    private $fileHandler;
    private $fileName;
    private $fileDirectory;
    private $filePrefix;
    private $fileSize;
    private $rotateHourly;

    /**
     * 创建指定文件保存目录和指定单个日志文件大小的 FileConsumer
     * 默认是按天切分，无默认大小切分
     * @param string $file_directory 日志文件保存目录. 默认为当前目录
     * @param int $file_size 单个日志文件大小. 单位 MB, 无默认大小
     * @param bool $rotate_hourly 是否按小时切分文件
     * @param string $file_prefix 生成的日志文件前缀
     */
    function __construct($file_directory = '.', $file_size = 0, $rotate_hourly = false, $file_prefix = '')
    {
        $this->fileDirectory = $file_directory;
        if (!is_dir($file_directory)) {
            mkdir($file_directory, 0777, true);
        }
        $this->fileSize = $file_size;
        $this->rotateHourly = $rotate_hourly;
        $this->filePrefix = $file_prefix;
        $this->fileName = $this->getFileName();
    }

    /**
     * 消费数据，将数据追加到本地日志文件
     * @param $message
     * @return bool|int
     */
    public function send($message)
    {
        $file_name = $this->getFileName();
        if ($this->fileHandler != null && $this->fileName != $file_name) {
            $this->close();
            $this->fileName = $file_name;
            $this->fileHandler = null;
        }
        if ($this->fileHandler === null) {
            $this->fileHandler = fopen($file_name, 'a+');
        }
        if (flock($this->fileHandler, LOCK_EX)) {
            $result = fwrite($this->fileHandler, $message . "\n");
            flock($this->fileHandler, LOCK_UN);
            return $result;
        }
    }

    public function close()
    {
        if ($this->fileHandler === null) {
            return false;
        }
        return fclose($this->fileHandler);
    }

    private function getFileName()
    {
        $date_format = $this->rotateHourly ? 'Y-m-d-H' : 'Y-m-d';
        $file_prefix = $this->filePrefix == '' ? '' : $this->filePrefix . '.';
        $file_base = $this->fileDirectory . '/' . $file_prefix . 'log.' . date($date_format, time()) . "_";
        $count = 0;
        $file_complete = $file_base . $count;
        if ($this->fileSize > 0) {
            while (file_exists($file_complete) && $this->fileSizeOut($file_complete)) {
                $count += 1;
                $file_complete = $file_base . $count;
            }
        }
        return $file_complete;
    }

    public function fileSizeOut($fp)
    {
        clearstatcache();
        $fpSize = filesize($fp) / (1024 * 1024);
        if ($fpSize >= $this->fileSize) {
            return true;
        } else {
            return false;
        }
    }
}

/**
 * 批量实时地向TA服务器传输数据，不需要搭配传输工具. 不建议在生产环境中使用，不支持多线程
 */
class AfBatchConsumer extends AfAbstractConsumer
{
    private $url;
    private $appid;
    private $buffers;
    private $maxSize;
    private $requestTimeout;
    private $compress = true;
    private $retryTimes;
    private $isThrowException = false;
    private $cacheBuffers;
    private $cacheCapacity;

    /**
     * 创建给定配置的 BatchConsumer 对象
     * @param string $server_url 接收端 url
     * @param string $appid 项目 APP ID
     * @param int $max_size 最大的 flush 值，默认为 20
     * @param int $retryTimes 因网络问题发生失败时重试次数，默认为 3次
     * @param int $request_timeout http 的 timeout，默认 1000s
     * @param int $cache_capacity 最大缓存倍数，实际存储量为$max_size * $cache_multiple
     * @throws AfDataException
     */
    function __construct($server_url, $appid, $max_size = 20, $retryTimes = 3, $request_timeout = 1000, $cache_capacity = 50)
    {
        $this->buffers = array();
        $this->appid = $appid;
        $this->maxSize = $max_size;
        $this->retryTimes = $retryTimes;
        $this->requestTimeout = $request_timeout;
        $parsed_url = parse_url($server_url);
        $this->cacheBuffers = array();
        $this->cacheCapacity = $cache_capacity;
        if ($parsed_url === false) {
            throw new AfDataException("Invalid server url");
        }
        $this->url = $parsed_url['scheme'] . "://" . $parsed_url['host']
            . ((isset($parsed_url['port'])) ? ':' . $parsed_url['port'] : '')
            . '/sync_server';
    }

    public function __destruct()
    {
        $this->flush();
    }

    public function send($message)
    {
        $this->buffers[] = $message;
        if (count($this->buffers) >= $this->maxSize) {
            return $this->flush();
        }
    }

    public function flush($flag = false)
    {
        if (empty($this->buffers) && empty($this->cacheBuffers)) {
            return true;
        }
        if ($flag || count($this->buffers) >= $this->maxSize || count($this->cacheBuffers) == 0) {
            $sendBuffers = $this->buffers;
            $this->buffers = array();
            $this->cacheBuffers[] = $sendBuffers;
        }
        while (count($this->cacheBuffers) > 0) {
            $sendBuffers = $this->cacheBuffers[0];

            try {
                $this->doRequest($sendBuffers);
                array_shift($this->cacheBuffers);
                if ($flag) {
                    continue;
                }
                break;
            } catch (AfDataNetWorkException $netWorkException) {
                if (count($this->cacheBuffers) > $this->cacheCapacity) {
                    array_shift($this->cacheBuffers);
                }

                if ($this->isThrowException) {
                    throw $netWorkException;
                }
                return false;
            } catch (AfDataException $dataException) {
                array_shift($this->cacheBuffers);

                if ($this->isThrowException) {
                    throw $dataException;
                }
                return false;
            }
        }

        return true;
    }

    public function close()
    {
        $this->flush(true);
    }

    public function setCompress($compress = true)
    {
        $this->compress = $compress;
    }

    public function setFlushSize($max_size = 20)
    {
        $this->maxSize = $max_size;
    }

    public function openThrowException()
    {
        $this->isThrowException = true;
    }

    private function doRequest($message_array)
    {
        $ch = curl_init($this->url);
        //参数设置
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 6000);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, $this->requestTimeout);

        if ($this->compress) {
            $data = gzencode("[" . implode(", ", $message_array) . "]");
        } else {
            $data = "[" . implode(", ", $message_array) . "]";
        }
        $compressType = $this->compress ? "gzip" : "none";
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        //headers
        curl_setopt($ch, CURLOPT_HTTPHEADER, array("AF-Integration-Type:PHP", "AF-Integration-Version:" . SDK_VERSION,
            "AF-Integration-Count:" . count($message_array), "appid:" . $this->appid, "compress:" . $compressType, 'Content-Type: text/plain'));

        //https
        $pos = strpos($this->url, "https");
        if ($pos === 0) {
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        }

        //发送请求
        $curreyRetryTimes = 0;
        while ($curreyRetryTimes++ < $this->retryTimes) {
            $result = curl_exec($ch);
            if (!$result) {
                echo new AfDataNetWorkException("Cannot post message to server , error --> " . curl_error(($ch)));
                continue;
            }
            //解析返回值
            $json = json_decode($result, true);

            $curl_info = curl_getinfo($ch);

            curl_close($ch);
            if ($curl_info['http_code'] == 200) {
                if ($json['code'] == 0) {
                    return;
                } else if ($json['code'] == -1) {
                    throw new AfDataException("传输数据失败，数据格式不合法, code = -1");
                } else if ($json['code'] == -2) {
                    throw new AfDataException("传输数据失败，APP ID 不合法, code = -2");
                } else if ($json['code'] == -3) {
                    throw new AfDataException("传输数据失败，非法上报 IP, code = -3");
                } else {
                    throw new AfDataException("传输数据失败 code = " . $json['code']);
                }
            } else {
                echo new AfDataNetWorkException("传输数据失败  http_code: " . $curl_info['http_code']);
            }
        }
        throw new AfDataNetWorkException("传输数据重试" . $this->retryTimes . "次后仍然失败！");
    }
}

/**
 * 逐条传输数据，如果发送失败则抛出异常
 */
class AfDebugConsumer extends AfAbstractConsumer
{
    private $url;
    private $appid;
    private $requestTimeout;
    private $writerData = true;

    /**
     * 创建给定配置的 DebugConsumer 对象
     * @param string $server_url 接收端 url
     * @param string $appid 项目 APP ID
     * @param int $request_timeout http 的 timeout，默认 1000s
     * @throws AfDataException
     */
    function __construct($server_url, $appid, $request_timeout = 1000)
    {
        $parsed_url = parse_url($server_url);
        if ($parsed_url === false) {
            throw new AfDataException("Invalid server url");
        }

        $this->url = $parsed_url['scheme'] . "://" . $parsed_url['host']
            . ((isset($parsed_url['port'])) ? ':' . $parsed_url['port'] : '')
            . '/data_debug';

        $this->appid = $appid;
        $this->requestTimeout = $request_timeout;
    }

    public function send($message)
    {
        return $this->doRequest($message);
    }

    public function setDebugOnly($writer_data = true)
    {
        $this->writerData = $writer_data;
    }

    public function close()
    {
    }

    private function doRequest($message)
    {
        $ch = curl_init($this->url);
        $dryRun = $this->writerData ? 0 : 1;
        $data = "source=server&appid=" . $this->appid . "&dryRun=" . $dryRun . "&data=" . urlencode($message);

        //参数设置
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 6000);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, $this->requestTimeout);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);

        //https
        $pos = strpos($this->url, "https");
        if ($pos === 0) {
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        }

        //发送请求
        $result = curl_exec($ch);
        if (!$result) {
            throw new AfDataNetWorkException("Cannot post message to server , error -->" . curl_error(($ch)));
        }
        //解析返回值
        $json = json_decode($result, true);

        $curl_info = curl_getinfo($ch);

        curl_close($ch);
        if ($curl_info['http_code'] == 200) {
            if ($json['errorLevel'] == 0) {
                return true;
            } else {
                echo "\nUnexpected Return Code " . $json['errorLevel'] . " for: " . $message . "\n";
                throw new AfDataException(print_r($json));
            }
        } else {
            throw new AfDataNetWorkException("传输数据失败. HTTP code: " . $curl_info['http_code'] . "\t return content :" . $result);
        }
    }
}