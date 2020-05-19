package ru.andrey.kvstorage.app;

import ru.andrey.kvstorage.app.domain.Post;
import ru.andrey.kvstorage.app.domain.PostMapper;
import ru.andrey.kvstorage.app.repo.PostRepositoryImpl;
import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.SocketConnection;
import ru.andrey.kvstorage.jclient.repository.KvsRepository;

public class Main {
    public static void main(String[] args) {
        KvsClient client = new SimpleKvsClient(
                "test_3",
                SocketConnection::new);

        KvsRepository<Post> postRepository = new PostRepositoryImpl(new PostMapper(), () -> client);
        Post post = postRepository.get("1");
        Post post1 = postRepository.store("2", new Post("Test", "dakenviy", "Good content"));
        Post post2 = postRepository.get("2");
        System.out.println(post);
        System.out.println(post1);
        System.out.println(post2);
    }
}
