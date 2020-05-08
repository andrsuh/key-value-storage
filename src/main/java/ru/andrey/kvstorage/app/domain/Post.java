package ru.andrey.kvstorage.app.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class Post {
    private final String getName;
    private final String user;
    private final String content;
}
