package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.console.impl.CreateDatabaseCommand;
import ru.andrey.kvstorage.server.console.impl.CreateTableCommand;
import ru.andrey.kvstorage.server.console.impl.ReadKeyCommand;
import ru.andrey.kvstorage.server.console.impl.UpdateKeyCommand;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;

public enum DatabaseCommands {

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


    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, String... args);
}
