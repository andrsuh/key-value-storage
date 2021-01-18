package ru.andrey.kvstorage.jclient.repository;

public interface KvsRepository<E> {
    E get(String id);

    E store(String id, E entity);
}
