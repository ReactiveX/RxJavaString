/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rx.observables;

import java.util.Queue;
import java.util.concurrent.atomic.*;
import java.util.regex.Pattern;

import rx.*;
import rx.Observable.OnSubscribe;
import rx.exceptions.Exceptions;
import rx.internal.operators.BackpressureUtils;
import rx.internal.util.atomic.SpscAtomicArrayQueue;
import rx.internal.util.unsafe.*;

/**
 * Split a sequence of strings based on a Rexexp pattern spanning subsequent
 * items if necessary.
 */
final class ObservableSplit implements OnSubscribe<String> {

    final Observable<String> source;

    final Pattern pattern;
    
    final int bufferSize;

    ObservableSplit(Observable<String> source, Pattern pattern, int bufferSize) {
        this.source = source;
        this.pattern = pattern;
        this.bufferSize = bufferSize;
    }
    
    @Override
    public void call(Subscriber<? super String> t) {
        SplitSubscriber parent = new SplitSubscriber(t, pattern, bufferSize);
        t.add(parent.requested);
        t.setProducer(parent.requested);

        source.unsafeSubscribe(parent);
    }

    static final class SplitSubscriber extends Subscriber<String> {
        
        final Subscriber<? super String> actual;
        
        final Pattern pattern;
        
        final Requested requested;
        
        final AtomicInteger wip;
        
        final int limit;
        
        final Queue<String[]> queue;

        String[] current;

        String leftOver;

        int index;

        int produced;

        int empty;

        volatile boolean done;
        Throwable error;

        volatile boolean cancelled;

        SplitSubscriber(Subscriber<? super String> actual, Pattern pattern, int bufferSize) {
            this.actual = actual;
            this.pattern = pattern;
            this.limit = bufferSize - (bufferSize >> 2);
            this.requested = new Requested();
            this.wip = new AtomicInteger();
            if (UnsafeAccess.isUnsafeAvailable()) {
                this.queue = new SpscArrayQueue<String[]>(bufferSize);
            } else {
                this.queue = new SpscAtomicArrayQueue<String[]>(bufferSize);
            }
            request(bufferSize);
        }

        @Override
        public void onNext(String t) {
            String lo = leftOver;
            String[] a;
            try {
                if (lo == null || lo.isEmpty()) {
                    a = pattern.split(t, -1);
                } else {
                    a = pattern.split(lo + t, -1);
                }
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                unsubscribe();
                onError(ex);
                return;
            }

            if (a.length == 0) {
                leftOver = null;
                request(1);
                return;
            } else
            if (a.length == 1) {
                leftOver = a[0];
                request(1);
                return;
            }
            leftOver = a[a.length - 1];
            queue.offer(a);
            drain();
        }

        @Override
        public void onError(Throwable e) {
            if (done) {
//                RxJavaHooks.onError(e); RxJava 1.2+ required
                return;
            }
            String lo = leftOver;
            if (lo != null && !lo.isEmpty()) {
                leftOver = null;
                queue.offer(new String[] { lo, null });
            }
            error = e;
            done = true;
            drain();
        }

        @Override
        public void onCompleted() {
            if (!done) {
                done = true;
                String lo = leftOver;
                if (lo != null && !lo.isEmpty()) {
                    leftOver = null;
                    queue.offer(new String[] { lo, null });
                }
                drain();
            }
        }

        void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }

            Queue<String[]> q = queue;

            int missed = 1;
            int consumed = produced;
            String[] array = current;
            int idx = index;
            int emptyCount = empty;

            Subscriber<? super String> a = actual;

            for (;;) {
                long r = requested.get();
                long e = 0;

                while (e != r) {
                    if (cancelled) {
                        current = null;
                        q.clear();
                        return;
                    }

                    boolean d = done;

                    if (array == null) {
                        array = q.poll();
                        if (array != null) {
                            current = array;
                            if (++consumed == limit) {
                                consumed = 0;
                                request(limit);
                            }
                        }
                    }

                    boolean empty = array == null;

                    if (d && empty) {
                        current = null;
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onCompleted();
                        }
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    if (array.length == idx + 1) {
                        array = null;
                        current = null;
                        idx = 0;
                        continue;
                    }

                    String v = array[idx];

                    if (v.isEmpty()) {
                        emptyCount++;
                        idx++;
                    } else {
                        while (emptyCount != 0 && e != r) {
                            if (cancelled) {
                                current = null;
                                q.clear();
                                return;
                            }
                            a.onNext("");
                            e++;
                            emptyCount--;
                        }

                        if (e != r && emptyCount == 0) {
                            a.onNext(v);

                            e++;
                            idx++;
                        }
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        current = null;
                        q.clear();
                        return;
                    }

                    boolean d = done;

                    if (array == null) {
                        array = q.poll();
                        if (array != null) {
                            current = array;
                            if (++consumed == limit) {
                                consumed = 0;
                                request(limit);
                            }
                        }
                    }

                    boolean empty = array == null;

                    if (d && empty) {
                        current = null;
                        Throwable ex = error;
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onCompleted();
                        }
                        return;
                    }
                }

                if (e != 0L) {
                    BackpressureUtils.produced(requested, e);
                }

                empty = emptyCount;
                produced = consumed;
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        void cancel() {
            cancelled = true;
            unsubscribe();
            if (wip.getAndIncrement() == 0) {
                current = null;
                queue.clear();
            }
        }
        
        boolean isCancelled() {
            return isUnsubscribed();
        }
        
        final class Requested extends AtomicLong implements Producer, Subscription {

            private static final long serialVersionUID = 3399839515828647345L;

            @Override
            public void request(long n) {
                if (n > 0) {
                    BackpressureUtils.getAndAddRequest(this, n);
                    drain();
                }
                else if (n < 0) {
                    throw new IllegalArgumentException("n >= 0 required but it was " + n);
                }
            }
            
            @Override
            public boolean isUnsubscribed() {
                return isCancelled();
            }
            
            @Override
            public void unsubscribe() {
                cancel();
            }
        }
    }
}
