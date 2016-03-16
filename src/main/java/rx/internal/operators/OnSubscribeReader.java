package rx.internal.operators;

import rx.Observer;
import rx.observables.SyncOnSubscribe;

import java.io.IOException;
import java.io.Reader;

public final class OnSubscribeReader extends SyncOnSubscribe<Reader, String> {

    private final Reader reader;
    private final int size;

    public OnSubscribeReader(Reader reader, int size) {
        this.reader = reader;
        this.size = size;
    }

    @Override
    protected Reader generateState() {
        return this.reader;
    }

    @Override
    protected Reader next(Reader state, Observer<? super String> observer) {
        char[] buffer = new char[size];
        try {
            int count = state.read(buffer);
            if (count == -1)
                observer.onCompleted();
            else
                observer.onNext(String.valueOf(buffer, 0, count));
        } catch (IOException e) {
            observer.onError(e);
        }
        return state;
    }
}
