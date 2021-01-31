package ru.andrey.kvstorage.app;

import ru.andrey.kvstorage.app.domain.Post;
import ru.andrey.kvstorage.app.domain.PostMapper;
import ru.andrey.kvstorage.app.repo.PostRepositoryImpl;
import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.ConnectionConfig;
import ru.andrey.kvstorage.jclient.connection.ConnectionPool;
import ru.andrey.kvstorage.jclient.connection.SocketKvsConnection;
import ru.andrey.kvstorage.jclient.repository.KvsRepository;

public class Main {
//    public static void main(String[] args) throws InterruptedException {
//        ConnectionPool connectionPool = new ConnectionPool(new ConnectionConfig());
//
//        KvsClient client = new SimpleKvsClient(
//                "test_3",
//                connectionPool::getClientConnection);
//
//        KvsRepository<Post> postRepository = new PostRepositoryImpl(new PostMapper(), () -> client);
//
////        Post post = postRepository.get("1");
//        Post post1 = postRepository.set("2", new Post("Test", "dakenviy", "Good content"));
//        Post post2 = postRepository.get("2");
////        System.out.println(post);
////        System.out.println(post1);
////        System.out.println(post2);
//
//        Post post = postRepository.get("1");
//        System.out.println(post);
//    }

    public static void main(String[] args) throws InterruptedException {
        ConnectionPool connectionPool = new ConnectionPool(new ConnectionConfig());

        KvsClient client = new SimpleKvsClient(
                "test_3",
                () -> new SocketKvsConnection(new ConnectionConfig()));

        KvsRepository<Post> postRepository = new PostRepositoryImpl(new PostMapper(), () -> client);

//        Post post = postRepository.get("1");
        Post post1 = postRepository.set("2", new Post("Test", "dakenviy", "Good content"));
//        Post post2 = postRepository.get("2");
//        System.out.println(post);
//        System.out.println(post1);
//        System.out.println(post2);

//        Post post = postRepository.get("1");
        System.out.println(post1);
    }
}
