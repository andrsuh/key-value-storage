package ru.andrey.kvstorage.jclient.repository;

import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.mapper.EntityMapper;

import java.util.function.Supplier;

public interface SpecifiedKvsRepository<E> extends KvsRepository<E> {

    Class<E> getSpecificationType();

    default String getTableName() {
        return getSpecificationType().getSimpleName();
    }

    EntityMapper<E> getMapper();

    Supplier<KvsClient> getClientFactory();

    @Override
    default E get(String id) {
        if (id == null) {
            throw new IllegalArgumentException("null id passed");
        }

        var client = getClientFactory().get();
        return getMapper().mapToObject(client.get(getTableName(), id));
    }

    @Override
    default E upsert(String id, E entity) {
        if (id == null) {
            throw new IllegalArgumentException("null id passed");
        }
        if (entity == null) {
            throw new IllegalArgumentException("null entity passed");
        }

        var client = getClientFactory().get();
        String prevValue = client.upsert(getTableName(), id, getMapper().mapToString(entity));

        return prevValue == null ? null : getMapper().mapToObject(prevValue);
    }
}
