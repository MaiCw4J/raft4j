package com.stephen;

import com.stephen.constanst.ReadOnlyOption;
import lombok.Getter;

public class ReadOnly {

    @Getter
    private ReadOnlyOption option;
    private ReadOnlyOption pending_read_index;
    private ReadOnlyOption read_index_queue;

//    pub option: ReadOnlyOption,
//    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
//    pub read_index_queue: VecDeque<Vec<u8>>,


    public ReadOnly(ReadOnlyOption option) {
        this.option = option;
//        pending_read_index: HashMap::default(),
//                read_index_queue: VecDeque::new(),
    }


}
