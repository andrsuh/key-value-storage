package ru.andrey.kvstorage.app.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.andrey.kvstorage.jclient.mapper.EntityMapper;

import java.io.IOException;

public class PostMapper implements EntityMapper<Post> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Post mapToObject(String s) {
        try {
            return objectMapper.readValue(s, Post.class);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot deserialize string to post: " + s, e);
        }
    }

    @Override
    public String mapToString(Post entity) {
        try {
            return objectMapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize port to json: " + entity, e);
        }
    }
}
