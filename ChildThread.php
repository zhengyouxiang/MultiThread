/**
 * @description: 子进程抽象类
 * @author: 俊杰Jerry<bupt1987@gmail.com>
 * @date: 2013-9-12
 * @version: 1.0
 */
abstract class ChildThread{

    const MSG_TYPE_FILE = 0;
    const MSG_TYPE_SYSVMSG = 1;
    const MSG_TYPE_REDIS = 2;
    
    private $per_msg_size = 10240;
    private $tmp_path;
    private $msg_prefix_key;
    private $msg_queue;
    private $msg_type;
    private $redis_key;
    private $redis_host;
    private $redis_port;

    /**
     * 构造函数
     * 
     * @param array $arg            
     * @param int msg_type 0:文件方式返回结果，1:systemvmsg方式返回结果，2:redis方式返回结果
     * @param int child_size 子线程个数
     * @param string tmp_path 文件方式
     * @param string msg_prefix_key msg的前缀
     * @param string redis_host redis地址，例如：127.0.0.1
     * @param int redis_port redis端口，例如：6379
     * @param int return_size 返回内容的长度，单位byte
     * 强烈建议使用redis做消息中间件
     */
    protected function __construct($arg = array()){
        $this->msg_type = isset($arg['msg_type']) ? $arg['msg_type'] : 0;
        $this->msg_prefix_key = isset($arg['msg_prefix_key']) ? $arg['msg_prefix_key'] : 'sfpid_';
        switch ($this->msg_type) {
            case ChildThread::MSG_TYPE_SYSVMSG:
                if(!function_exists('msg_get_queue')){
                    exit('msg_get_queue函数不存在');
                }
                empty($arg['return_size']) && $arg['return_size'] = $this->per_msg_size;
                empty($arg['child_size']) && $arg['child_size'] = 10;
                $message_queue = msg_get_queue(time() * 100 + rand(0, 99));
                if($message_queue !== false){
                    $this->msg_queue = $message_queue;
                }else{
                    exit('创建队列失败');
                }
                msg_set_queue($this->msg_queue, array ('msg_qbytes'=>$arg['child_size'] * $arg['return_size']));
                break;
            case ChildThread::MSG_TYPE_REDIS:
                if(!class_exists('Redis')){
                    exit('Redis没有加载');
                }
                $this->redis_host = !empty($arg['redis_host']) ? $arg['redis_host'] : '127.0.0.1';
                $this->redis_port = !empty($arg['redis_port']) ? $arg['redis_port'] : '6379';
                $this->redis_key = $this->msg_prefix_key . (empty($arg['redis_key']) ? __CLASS__ : $arg['redis_key']);
                $this->msg_queue = new Redis();
                break;
            default:
                $this->tmp_path = !empty($arg['tmp_path']) ? $arg['tmp_path'] : '/tmp';
                break;
        }
    }

    /**
     * 摧毁对象
     * @param $arg
     */
    public function _destruct(){
        if($this->msg_type == ChildThread::MSG_TYPE_SYSVMSG){
            msg_remove_queue($this->msg_queue);
        }elseif($this->msg_type == ChildThread::MSG_TYPE_REDIS){
            $this->msg_queue->delete($this->redis_key);
        }
    }

    /**
     * 导入任务
     * 
     * @return array
     */
    public function _addTask(){
        return array ();
    }

    /**
     * 处理返回结果
     * 
     * @param string $rs            
     */
    public function _processReturn($rs){
    }

    /**
     * 结果返回
     * 
     * @param int $cid 子线程id
     * @param $rs 结果            
     */
    public function _sent($cid, $rs){
        switch($this->msg_type){
            case ChildThread::MSG_TYPE_SYSVMSG:
                // 向消息队列中写
                if(!msg_send($this->msg_queue, 1, $rs)){
                    exit("发送失败 : {$cid} => {$rs}\n");
                }
                break;
            case ChildThread::MSG_TYPE_REDIS:
                $this->msg_queue->connect($this->redis_host, $this->redis_port);
                if($this->msg_queue->lPush($this->redis_key, $rs) === false){
                    exit("插入redis失败 : {$cid}|{$this->redis_key} => {$rs}\n");
                }
                break;
            default:
                file_put_contents($this->tmp_path . '/' . $this->msg_prefix_key . $cid, $rs);
                break;
        }
    }

    /**
     * 获取结果
     * 
     * @param int $cid 子线程id
     * @return string
     */
    public function _get($cid){
        switch($this->msg_type){
            case ChildThread::MSG_TYPE_SYSVMSG:
                // 从消息队列中读
                if(msg_receive($this->msg_queue, 0, $message_type, $this->per_msg_size, $message, true, MSG_IPC_NOWAIT)){
                    return $message;
                }
                break;
            case ChildThread::MSG_TYPE_REDIS:
                $this->msg_queue->connect($this->redis_host, $this->redis_port);
                if(($message = $this->msg_queue->rPop($this->redis_key)) !== false){
                    return $message;
                }
                break;
            default:
                $filename = $this->tmp_path . '/' . $this->msg_prefix_key . $cid;
                if(is_file($filename)){
                    $message = file_get_contents($filename);
                    unlink($filename);
                    return $message;
                }
                break;
        }
        return null;
    }

    /**
     * 抽象运行
     */
    public abstract function _fork($arg);
}
