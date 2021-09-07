package ru.andrey.kvstorage.server.console;

import ru.andrey.kvstorage.resp.object.RespObject;
import ru.andrey.kvstorage.server.console.impl.*;
import ru.andrey.kvstorage.server.logic.impl.DatabaseImpl;

import java.util.List;

/**
 * Перечисление команд. Пример создания и использования:
 * DatabaseCommands.valueOf("GET_KEY").getCommand(env, commandArgs).execute()
 */
public enum DatabaseCommands {

    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new CreateDatabaseCommand(env, DatabaseImpl::create, commandArgs);
        }
    },
    CREATE_TABLE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new CreateTableCommand(env, commandArgs);
        }
    },
    SET_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new SetKeyCommand(env, commandArgs);
        }
    },
    GET_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new GetKeyCommand(env, commandArgs);
        }
    },
    DELETE_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
            return new DeleteKeyCommand(env, commandArgs);
        }
    };

    /**
     * Возвращает созданную команду
     */
    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, List<RespObject> commandArgs);
}
