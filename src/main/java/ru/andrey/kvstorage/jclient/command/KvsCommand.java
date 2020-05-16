package ru.andrey.kvstorage.jclient.command;

import java.util.List;

public interface KvsCommand {

    List<String> asList();

//    String getName(); // todo think about this we can use it in this#toApiBytesRepresentation
//    List<String> getArgs();

    default byte[] toApiBytesRepresentation() {
        return String.join(" ", asList()).getBytes(); // todo sukhoa selialize to array (RESP protocol)
//        List<String> list = asList();
//        String startArray = "#" + list.size() + separator;
//        return list.stream()
//                .map(t -> "$" + t + separator)
//                .collect(Collectors.joining()).getBytes();
    }
}
