package com.sshtools.gardensched;

import java.io.Serializable;
import java.util.concurrent.Callable;

public interface SerializableCallable<RESULT> extends Callable<RESULT>, Serializable  {
}