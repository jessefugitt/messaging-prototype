/*
    Copyright 2015 Kaazing Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package org.kaazing.messaging.example;

import org.kaazing.messaging.common.message.Message;
import org.kaazing.messaging.common.destination.Pipe;
import org.kaazing.messaging.driver.MessagingDriver;
import org.kaazing.messaging.rx.MessageConsumerRxAdapter;
import rx.Observable;
import rx.Subscription;

import static rx.observables.MathObservable.averageDouble;

public class RxJavaPipeConsumerExample
{
    public static void main(String[] args)
    {
        MessagingDriver driver = new MessagingDriver();

        Pipe pipe = new Pipe("aeron:udp?remote=127.0.0.1:40124", 10);

        Observable<Message> observableMessageConsumer = MessageConsumerRxAdapter.createObservable(driver, pipe);

        Subscription subscription1 = observableMessageConsumer.subscribe(
                (message) -> System.out.println("Received message with payload: " + message.getBuffer().getInt(message.getBufferOffset()))
        );


        int windowCount = 3;
        Observable<Double> movingAverageObservable = observableMessageConsumer.map(
            message -> {
                return (double) (message.getBuffer().getInt(message.getBufferOffset()));
            }
        ).window(windowCount, 1).flatMap(
            window -> {
                return averageDouble(window);
            }
        );

        Subscription subscription2 = movingAverageObservable.subscribe(
                (average) -> System.out.println("Moving Average(last " + windowCount + ") = " + average)
        );



        int i = 0;
        while(i < 10) {
            try {
                Thread.sleep(1000);
                if(i == 6)
                {
                    subscription1.unsubscribe();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }

        subscription1.unsubscribe();
        subscription2.unsubscribe();
        driver.close();
    }
}
