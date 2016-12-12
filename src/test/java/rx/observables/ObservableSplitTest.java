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

import java.io.IOException;

import org.junit.Test;

import rx.Observable;
import rx.observers.TestSubscriber;

public class ObservableSplitTest {

    void test(String[] input, String pattern, String[] output, boolean rebatch) {
        TestSubscriber<String> ts = new TestSubscriber<String>();
        Observable<String> o = StringObservable.split(Observable.from(input), pattern);
        if (rebatch) {
            o = o.rebatchRequests(1);
        }
        o.subscribe(ts);

        ts.assertValues(output);
        ts.assertNoErrors();
        ts.assertCompleted();
    }
    
    @Test
    public void split1() {
        test(new String[] {"ab", ":cd", "e:fgh" }, ":", new String[] { "ab", "cde", "fgh" }, false);
    }

    @Test
    public void split1Request1() {
        test(new String[] {"ab", ":cd", "e:fgh" }, ":", new String[] { "ab", "cde", "fgh" }, true);
    }

    @Test
    public void split2() {
        test(new String[] { "abcdefgh" }, ":", new String[] { "abcdefgh" }, false);
    }

    @Test
    public void split2Request1() {
        test(new String[] { "abcdefgh" }, ":", new String[] { "abcdefgh" }, true);
    }

    @Test
    public void splitEmpty() {
        test(new String[] { }, ":", new String[] { }, false);
    }

    @Test
    public void splitError() {
        TestSubscriber<String> ts = new TestSubscriber<String>();
        Observable<String> o = StringObservable.split(Observable.<String>empty().concatWith(Observable.<String>error(new IOException())), ":");
        o.subscribe(ts);

        ts.assertNoValues();
        ts.assertNotCompleted();
        ts.assertError(IOException.class);
    }

    @Test
    public void splitExample1() {
        test(new String[] { "boo:and:foo" }, ":", new String[] { "boo", "and", "foo" }, false);
    }

    @Test
    public void splitExample1Request1() {
        test(new String[] { "boo:and:foo" }, ":", new String[] { "boo", "and", "foo" }, true);
    }

    @Test
    public void splitExample2() {
        test(new String[] { "boo:and:foo" }, "o", new String[] { "b", "", ":and:f" }, false);
    }

    @Test
    public void splitExample2Request1() {
        test(new String[] { "boo:and:foo" }, "o", new String[] { "b", "", ":and:f" }, true);
    }

    @Test
    public void split3() {
        test(new String[] { "abqw", "ercdqw", "eref" }, "qwer", new String[] { "ab", "cd", "ef" }, false);
    }

    @Test
    public void split3Buffer1Request1() {
        test(new String[] { "abqw", "ercdqw", "eref" }, "qwer", new String[] { "ab", "cd", "ef" }, true);
    }

    @Test
    public void split4() {
        test(new String[] { "ab", ":", "", "", "c:d", "", "e:" }, ":", new String[] { "ab", "c", "de" }, false);
    }

    @Test
    public void split4Request1() {
        test(new String[] { "ab", ":", "", "", "c:d", "", "e:" }, ":", new String[] { "ab", "c", "de" }, true);
    }

}
