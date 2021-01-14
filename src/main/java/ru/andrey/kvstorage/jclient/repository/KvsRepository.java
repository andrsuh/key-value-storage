package ru.andrey.kvstorage.jclient.repository;

public interface KvsRepository<E> {
    E get(String id);

    E upsert(String id, E entity);
}
