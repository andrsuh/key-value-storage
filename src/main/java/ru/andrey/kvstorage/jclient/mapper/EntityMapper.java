package ru.andrey.kvstorage.jclient.mapper;

public interface EntityMapper<E> {
    E mapToObject(String s);

    String mapToString(E entity);
}
