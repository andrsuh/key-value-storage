package ru.andrey.kvstorage.app.repo;

import ru.andrey.kvstorage.app.domain.Post;
import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.mapper.EntityMapper;
import ru.andrey.kvstorage.jclient.repository.SpecifiedKvsRepository;

import java.util.function.Supplier;

public class PostRepositoryImpl implements SpecifiedKvsRepository<Post> {
    private final EntityMapper<Post> mapper;
    private final Supplier<KvsClient> clientFactory;

    public PostRepositoryImpl(EntityMapper<Post> mapper, Supplier<KvsClient> clientFactory) {
        this.mapper = mapper;
        this.clientFactory = clientFactory;
    }

    @Override
    public Class<Post> getSpecificationType() {
        return Post.class;
    }

    @Override
    public EntityMapper<Post> getMapper() {
        return mapper;
    }

    @Override
    public Supplier<KvsClient> getClientFactory() {
        return clientFactory;
    }
}
