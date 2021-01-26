package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.server.console.impl.CreateDatabaseCommand;
import ru.andrey.kvstorage.server.console.impl.CreateTableCommand;
import ru.andrey.kvstorage.server.console.impl.GetKeyCommand;
import ru.andrey.kvstorage.server.console.impl.SetKeyCommand;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;

import java.util.List;

public enum DatabaseCommands {

    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<String> commandArgs) {
            return new CreateDatabaseCommand(env, DatabaseImpl::create, commandArgs);
        }
    },
    CREATE_TABLE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<String> commandArgs) {
            return new CreateTableCommand(env, commandArgs);
        }
    },
    SET_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<String> commandArgs) {
            return new SetKeyCommand(env, commandArgs);
        }
    },
    GET_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<String> commandArgs) {
            return new GetKeyCommand(env, commandArgs);
        }
    };


    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, List<String> commandArgs);
}
