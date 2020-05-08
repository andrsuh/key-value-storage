package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.console.impl.*;
import ru.andrey.kvstorage.server.initialization.Initializer;
import ru.andrey.kvstorage.server.initialization.impl.DatabaseInitializer;
import ru.andrey.kvstorage.server.initialization.impl.SegmentInitializer;
import ru.andrey.kvstorage.server.initialization.impl.TableInitializer;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;

public enum DatabaseCommands {

    INIT_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new InitializeDatabaseCommand(env, INITIALIZER, args);
        }
    },
    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new CreateDatabaseCommand(env, DatabaseImpl::create, args);
        }
    },
    CREATE_TABLE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new CreateTableCommand(env, args);
        }
    },
    UPDATE_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new UpdateKeyCommand(env, args);
        }
    },
    READ_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new ReadKeyCommand(env, args);
        }
    };

    private static final Initializer INITIALIZER = new DatabaseInitializer( // todo sukhoa this is temporary solution
            new TableInitializer(
                    new SegmentInitializer()
            )
    );

    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, String... args);
}
