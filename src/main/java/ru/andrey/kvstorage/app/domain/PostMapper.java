package ru.andrey.kvstorage.app.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.andrey.kvstorage.jclient.mapper.EntityMapper;

import java.io.IOException;

@Slf4j
public class PostMapper implements EntityMapper<Post> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Post mapToObject(String s) {
        try {
            log.debug("Mapping {} to Post", s);
            return objectMapper.readValue(s, Post.class);
        } catch (IOException e) {
            log.warn("Can't deserialize {} to post. Exception {}", s, e.getMessage());
            throw new IllegalStateException("Cannot deserialize string to post: " + s, e);
        }
    }

    @Override
    public String mapToString(Post entity) {
        try {
            log.debug("Mapping {} to String", entity);
            return objectMapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            log.warn("Can't serialize {} to String. Exception {}", entity, e.getMessage());
            throw new IllegalStateException("Cannot serialize post to json: " + entity, e);
        }
    }
}
