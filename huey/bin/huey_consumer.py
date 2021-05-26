#!/usr/bin/env python
# coding=utf-8

import logging
import os
import sys

from huey.consumer import Consumer
from huey.consumer_options import ConsumerConfig
from huey.consumer_options import OptionParserHandler
from huey.utils import load_class


def err(s):
    sys.stderr.write('\033[91m%s\033[0m\n' % s)


def load_huey(path):
    try:
        return load_class(path)
    except:
        cur_dir = os.getcwd()
        if cur_dir not in sys.path:
            sys.path.insert(0, cur_dir)
            return load_huey(path)
        err('Error importing %s' % path)
        raise


def consumer_main():
    # 使用optparse模块实现的命令行参数解析器
    parser_handler = OptionParserHandler()
    parser = parser_handler.get_option_parser()
    options, args = parser.parse_args()

    if len(args) == 0:
        err('Error:   missing import path to `Huey` instance')
        err('Example: huey_consumer.py app.queue.huey_instance')
        sys.exit(1)

    options = {k: v for k, v in options.__dict__.items()
               if v is not None}
    # 继承nametuple以实现的消费者进程配置类，添加了validate方法
    config = ConsumerConfig(**options)
    config.validate()

    # 从命令行参数中取huey实例的路径，导入路径并加载，获取huey实例
    # 加载过程中，所有被huey.task和huey.periodic_task装饰的函数都会被注册到huey上
    huey_instance = load_huey(args[0])

    # Set up logging for the "huey" namespace.
    logger = logging.getLogger('huey')
    config.setup_logger(logger)

    # 创建消费者实例
    consumer = huey_instance.create_consumer(**config.values)
    # 启动消费者
    consumer.run()


if __name__ == '__main__':
    # huey 消费进程启动
    if sys.version_info >= (3, 8) and sys.platform == 'darwin':
        import multiprocessing
        try:
            multiprocessing.set_start_method('fork')
        except RuntimeError:
            pass
    consumer_main()
