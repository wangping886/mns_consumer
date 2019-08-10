# mns_consumer
go/golang sdk  consumer message from mns.queue

````
mns_consumer 嵌入到已有服务中非常简单，通过NewConsumer()传入需要对每个消息处理的逻辑(hander方法)

便可以享用多协程并发执行任务。使用示例见main.go