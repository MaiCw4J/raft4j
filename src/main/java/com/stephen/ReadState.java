package com.stephen;

import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ReadState {
    /// The index of the read state.
    private long index;
    /// A datagram consisting of context about the request.
    private ByteString requestCtx;
}
