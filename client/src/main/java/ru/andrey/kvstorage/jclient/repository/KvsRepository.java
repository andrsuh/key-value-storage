package ru.andrey.kvstorage.jclient.repository;

import ru.andrey.kvstorage.jclient.exception.DatabaseExecutionException;

public interface KvsRepository<E> {
    E get(String id) throws DatabaseExecutionException;

    E set(String id, E entity) throws DatabaseExecutionException;
}
