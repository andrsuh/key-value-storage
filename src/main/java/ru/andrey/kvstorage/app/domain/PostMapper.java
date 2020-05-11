package ru.andrey.kvstorage.app.domain;

import ru.andrey.kvstorage.jclient.mapper.EntityMapper;

public class PostMapper implements EntityMapper<Post> {
    @Override
    public Post mapToObject(String s) {
        String[] postArray = s.split("#"); // todo use json serialization
        return new Post(postArray[0], postArray[1], postArray[2]);
    }

    @Override
    public String mapToString(Post entity) {
        return entity.getGetName() + "#" + entity.getUser() + "#" + entity.getContent();
    }
}
