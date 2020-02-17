package ru.andrey.kvstorage.console;

public enum DbCommandType {
    INIT_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return null;
        }
    },
    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new CreateDatabaseCommand(env, args);
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
