package ru.andrey.kvstorage.app.domain;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Post {
    private String title;
    private String user;
    private String content;
}
