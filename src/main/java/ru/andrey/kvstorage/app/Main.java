package ru.andrey.kvstorage.app;

import ru.andrey.kvstorage.app.domain.Post;
import ru.andrey.kvstorage.app.domain.PostMapper;
import ru.andrey.kvstorage.app.repo.PostRepositoryImpl;
import ru.andrey.kvstorage.jclient.DatabaseResponseParser;
import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.SocketConnection;
import ru.andrey.kvstorage.jclient.repository.KvsRepository;

public class Main {
    public static void main(String[] args) {
        KvsClient client = new SimpleKvsClient(
                "test_3",
                SocketConnection::new,
                new DatabaseResponseParser());

        KvsRepository<Post> postRepository = new PostRepositoryImpl(new PostMapper(), () -> client);
        Post post = postRepository.get("1");
        System.out.println(post);
    }
}
