const express = require('express');
const amqp = require('amqplib');

const app = express();

const QUEUE_NAME = 'task_queue';

async function processTask(task) {
  console.log('Задание обработано:', task);
  return { result: 'Результат обработки задания' };
}

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });
    channel.prefetch(1); 

    console.log('Микросервис М2 ожидает задания...');

    
    channel.consume(QUEUE_NAME, async (msg) => {
      const task = JSON.parse(msg.content.toString());
      const result = await processTask(task);

      console.log('Результат обработки: Пользователь', task.login, ' зарегестрирован с паролем ', task.password);

      
      channel.ack(msg);
    });
  } catch (err) {
    console.error('Ошибка при запуске микросервиса М2:', err);
  }
}

startWorker();

app.use(express.json());

app.post('/process', async (req, res) => {
  try {
    const task = req.body; 

    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });

    channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(task)), {
      persistent: true,
    });
    console.log('Задание отправлено в очередь:', task);
    res.status(200).json({ message: 'Задание отправлено в очередь' });
  } catch (err) {
    console.error('Ошибка обработки запроса:', err);
    res.status(500).json({ error: 'Ошибка обработки запроса' });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Микросервис М1 запущен на порту ${PORT}`);
});
