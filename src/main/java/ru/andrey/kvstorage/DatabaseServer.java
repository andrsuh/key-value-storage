package ru.andrey.kvstorage;

import ru.andrey.kvstorage.app.domain.Post;
import ru.andrey.kvstorage.app.domain.PostMapper;
import ru.andrey.kvstorage.app.repo.PostRepositoryImpl;
import ru.andrey.kvstorage.jclient.DatabaseResponseParser;
import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.client.SimpleKvsClient;
import ru.andrey.kvstorage.jclient.connection.DirectReferenceConnection;
import ru.andrey.kvstorage.jclient.repository.KvsRepository;
import ru.andrey.kvstorage.server.console.DatabaseCommandResult;
import ru.andrey.kvstorage.server.console.DatabaseCommands;
import ru.andrey.kvstorage.server.console.ExecutionEnvironment;
import ru.andrey.kvstorage.server.console.impl.ExecutionEnvironmentImpl;

import java.util.Arrays;

public class DatabaseServer {

    private final ExecutionEnvironment env;

    public DatabaseServer(ExecutionEnvironment env) {
        this.env = env;
    }

    public static void main(String[] args) {
        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl());

//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
//            while (true) {
//                DatabaseCommandResult commandResult = databaseServer.executeNextCommand(reader.readLine());
//
//                if (commandResult.isSuccess()) {
//                    System.out.println(commandResult.getResult().get());
//                }
//            }
//        } catch (IOException e) {
//            throw new IllegalStateException("Server disconnected due to exceptions while opening input stream", e);
//        }
        databaseServer.executeNextCommand("INIT_DATABASE test_3"); // todo sukhoa Make so that databases are getting initialised on startUP

        KvsClient client = new SimpleKvsClient(
                "test_3",
                () -> new DirectReferenceConnection(databaseServer),
                new DatabaseResponseParser());

        KvsRepository<Post> postRepository = new PostRepositoryImpl(new PostMapper(), () -> client);
        Post post = postRepository.get("1");
        System.out.println(post);
    }

    public byte[] executeNextCommandAndGetApiBytes(byte[] commandText) {
        return executeNextCommand(new String(commandText)).toApiBytes();
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        try {
            String[] commandInfo = commandText.split(" ");

            return DatabaseCommands.valueOf(commandInfo[0])
                    .getCommand(env, commandInfo)
                    .execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String message = e.getMessage() != null
                    ? e.getMessage()
                    : Arrays.toString(e.getStackTrace());
            return DatabaseCommandResult.error(message);
        }
    }
}
