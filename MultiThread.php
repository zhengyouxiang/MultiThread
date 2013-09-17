/**
 * @description: php多进程
 * @author: 俊杰Jerry<bupt1987@gmail.com>
 * @date: 2013-9-12
 * @version: 1.0
 */
class MultiThread{
    
    private $max_thread;
    private $current_thread_count = 0;
    private $obj;
    private $arg;
    private $sleep_time;
    private $need_return = false;
    private $total_run = 0;
    private $cid_arr = array();
    private $is_child = false;
    private $min_task_count;
    private $need_add_task = false;
    private $need_process_return;
    
    /**
     * 构造函数
     * @param object $obj执行对象
     * @param int|array|null $arg用于对象中的_fork方法所执行的参数。如:$arg,自动分解为:$obj->_fork($arg[0])、$obj->_fork($arg[1])...如果为null则会调用$obj->_addTask()导入新任务
     * @param int $max_thread最多创建子进程数
     * @param bool $need_return是否需要接收返回结果
     * @param bool $need_process_return是否处理返回结果，必须在$need_return为true时才生效，生效将会调用$obj->_processReturn($rs);
     * @param int $sleep_time检查子进程间隔时间，单位微秒
     */
    public function __construct(ChildThread $obj, $arg = null, $max_thread = 10, $need_return = false, $need_process_return = false, $sleep_time = 10000){
        if(is_object($obj)){
            $class = new ReflectionClass(get_class($obj));
            $parent = $class->getParentClass();
            if(!$parent || $parent->getName() != 'ChildThread'){
                exit('obj没有继承ChildThread类');
            }
        }else{
            exit('obj不是一个对象');
        }
        if(!is_array($arg) && !is_numeric($arg) && $arg !== null){
            exit('arg类型不正确');
        }
        $this->arg = $arg;
        $this->obj = $obj;
        $this->max_thread = $max_thread;
        $this->min_task_count = 2 * $this->max_thread;
        $this->need_return = $need_return;
        $this->sleep_time = $sleep_time;
        $this->need_process_return = $need_process_return;
        if($this->arg === null){
            $this->need_add_task = true;
            $this->addTask();
        }
    }

    /**
     * 引擎主启动方法
     * @return array 返回子进程执行结果;
     */
    public function run(){
        $spawns = $this->addRun();
        return $this->check($spawns);
    }
    
    /**
     * 加入任务
     */
    private function addTask(){
        if($this->need_add_task){
            if($this->arg === null){
                $this->arg = array();
            }
            while(count($this->arg) < $this->min_task_count){
                $temp_arg = $this->obj->_addTask();
                if(is_array($temp_arg) && !empty($temp_arg)){
                    $this->arg = array_merge($this->arg, $temp_arg);
                }else{
                    break;
                }
            }
        }
    }
    
    
    /**
     * 将任务加入到子进程
     * @return number
     */
    private function addRun(){
        if(!$this->arg){
            return 0;
        }
        if(is_array($this->arg)){
            $this->addTask();
            $i = 0;
            foreach($this->arg as $key=>$val){
                if($this->current_thread_count == $this->max_thread){
                    break;
                }
                $i++;
                $cid = $this->spawn($this->obj, $val);
                $this->cid_arr[$cid] = $cid;
                unset($this->arg[$key]);
                $this->current_thread_count++;
            }
        }elseif(is_numeric($this->arg)){
            for($i = 0;$i < $this->arg;$i++){
                if($this->current_thread_count == $this->max_thread){
                    break;
                }
                $cid = $this->spawn($this->obj);
                $this->cid_arr[$cid] = $cid;
                $this->arg--;
                $this->current_thread_count++;
            }
        }
        $this->total_run += $i;
        return $i;
    }
    
    /**
     * 主进程控制方法
     * 1、$data收集子进程运行结果及数据，并用于最终返回
     * 2、直到所有子进程执行完毕，清理子进程资源
     * @param string|array $arg用于对应每个子进程的ID
     * @return array 返回子进程执行结果;
     *            
     */
    private function check($i){
        $this->need_return && $data = array ();
        for($id = 0;$id < $i;$id++){
            while(!($cid = pcntl_waitpid(-1, $status, WNOHANG))){
                usleep($this->sleep_time);
            }
            unset($this->cid_arr[$cid]);
            if($this->need_return){
                if($this->need_process_return){
                    $this->obj->_processReturn($this->obj->_get($cid));
                }else{
                    $data[] = $this->obj->_get($cid);
                }
            }
            $this->current_thread_count--;
            $i += $this->addRun();
        }
        if($i && $this->need_return){
            return $data;
        }
    }
    
    /**
     * 子进程执行方法
     * 1、pcntl_fork 生成子进程
     * 2、执行结果返回
     * 3、posix_kill杀死当前进程
     * @param object $obj            
     * @param object $param用于输入对象$obj方法'_fork'执行参数
     */
    private function spawn(ChildThread $obj, $param = null){
        $pid = pcntl_fork();
        if($pid === 0){
            $this->is_child = true;
            $cid = getmypid();
            $rs = $obj->_fork($param);
            if($this->need_return && $rs !== null){
                $obj->_sent($cid, $rs);
            }
            posix_kill($cid, SIGCHLD);
            exit();
        }else{
            return $pid;
        }
    }
    
    /**
     * 主进程停止时，kill子进程，删除队列
     */
    public function __destruct(){
        if(!$this->is_child){
            if($this->need_return){
                $this->obj->_destruct();
            }
            $time = '[' . date('Y-m-d H:i:s') . '] ';
            if(!empty($this->cid_arr)){
                foreach ($this->cid_arr as $cid){
                    echo $time . '异常退出,杀死子进程 : ' . $cid . "\n";
                    posix_kill($cid, SIGKILL);
                    usleep(30000);
                }
            }
        }
    }
    
}

?>
