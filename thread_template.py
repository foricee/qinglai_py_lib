import multiprocessing as mp
from queue import Empty
import time

class ProducerAndConsumer():

    def __init__(self, max_workers=5, queue_size=1000, queue_wait_second=5, print_step=100,
                 produce_input_data_func=None,
                 consume_input_data_func=None,
                 consume_output_data_func=None,
                 reduce_data_func=None,
                 reduce_data=None,
                 output_file_name=None,
                 output_file_write_mode='a',
                 ):
        self.max_workers = max_workers

        self.queue_size = queue_size
        self.queue_wait_second = queue_wait_second # 远大于单条数据处理时间即可，比如10倍于

        self.input_data_queue = mp.Queue(self.queue_size)
        self.output_data_queue = mp.Queue(self.queue_size)

        self.print_step = print_step

        self.output_count = 0

        if produce_input_data_func is not None:
            self.produce_input_data_func = produce_input_data_func
        else:
            raise Exception('error: produce_input_data_func not provided')

        if consume_input_data_func is not None:
            self.consume_input_data_func = consume_input_data_func
        else:
            raise Exception('error: consume_input_data_func not provided')

        if consume_output_data_func is not None:
            self.consume_output_data_func = consume_output_data_func
        else:
            raise Exception('error: consume_output_data_func not provided')

        self.reduce_data_func = reduce_data_func
        self.reduce_data = reduce_data
        
        self.output_file_name = output_file_name
        self.output_file_write_mode = output_file_write_mode

    # 单线程读取数据，一般做一些数据的预处理
    def produce_input_data(self):
        i = 0
        start = time.time()
        for data in self.produce_input_data_func():
            self.input_data_queue.put(data)
            i += 1
            if i % self.print_step == 0:
                print('produced input ', i, 'using time', time.time() - start)
                start = time.time()
        print('produce_input_data done')

    # 多线程加工数据，一般是核心逻辑，耗时的部分
    def consume_input_data(self, worker_id):
        while True:
            try:
                input_data = self.input_data_queue.get(timeout=self.queue_wait_second)
                output_data = self.consume_input_data_func(input_data)
                self.output_data_queue.put(output_data)
            except Empty as e:
                print(f'consume_input_data worker {worker_id} done')
                break


    # 单线程读取上一步结果，做一些处理生成最终结果。
    # 最后如果是想把结果写入文件，可以把数据放到reduce_data内，在reduce_data_func阶段写入
    def consume_output_data(self):
        i = 0
        start = time.time()
        iter_start = start
        can_print_log = False
        
        if self.output_file_name:
            output_file = open(self.output_file_name, self.output_file_write_mode)
        else:
            output_file = None

        while True:
            try:
                output_data = self.output_data_queue.get(timeout=self.queue_wait_second)
                self.output_count += self.consume_output_data_func(output_data, self.reduce_data, output_file, can_print_log)
                can_print_log = False

                i += 1
                if i % self.print_step == 0:
                    print('produced output', self.output_count, 
                          'using time', time.time() - iter_start,
                          'all time', time.time() - start,
                          )
                    can_print_log = True
                    iter_start = time.time()
            except Empty as e:
                print('consume_output_data done')
                break

        if self.reduce_data_func is not None:
            self.reduce_data_func(self.reduce_data)
            print('reduce data done')

    # 总体调度函数
    def start(self):
        start_time = time.time()
        produce_process = mp.Process(target=self.produce_input_data)
        consume_input_processes = []
        for worker_id in range(self.max_workers):
            consume_input_processes.append(mp.Process(target=self.consume_input_data, args=(worker_id,)))

        produce_process.start()
        for process in consume_input_processes:
            process.start()

        self.consume_output_data()

        print('time used(second)', time.time() - start_time)


def produce_input_data_func_demo():
    for i in range(300):
        yield i
        print('producing', i)

def consume_input_data_func_demo(input_data):
    print('consuming input data', input_data)
    return input_data

# 需要返回一个数字
def consume_output_data_func_demo(output_data, reduce_data, output_file, can_print_log):
    print('consuming output data', output_data)
    if output_file:
        output_file.write('something')
    return 1

def reduce_data_func_demo(reduce_data):
    print('reduce data', reduce_data)

if __name__ == '__main__':
    producerAndConsumer =  ProducerAndConsumer(
        produce_input_data_func=produce_input_data_func_demo,
        consume_input_data_func=consume_input_data_func_demo,
        consume_output_data_func=consume_output_data_func_demo,
        reduce_data_func=reduce_data_func_demo,
        max_workers=5,
        queue_size=1000,
        queue_wait_second=5,
        print_step=1000,
    )
    producerAndConsumer.start()