package ru.andrey.kvstorage.jclient.repository;

import ru.andrey.kvstorage.jclient.client.KvsClient;
import ru.andrey.kvstorage.jclient.exception.DatabaseExecutionException;
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
    default E get(String id) throws DatabaseExecutionException {
        if (id == null) {
            throw new IllegalArgumentException("null id passed");
        }

        var client = getClientFactory().get();
        String result = client.get(getTableName(), id);
        return result != null ? getMapper().mapToObject(result) : null;
    }

    @Override
    default E set(String id, E entity) throws DatabaseExecutionException {
        if (id == null) {
            throw new IllegalArgumentException("null id passed");
        }
        if (entity == null) {
            throw new IllegalArgumentException("null entity passed");
        }

        var client = getClientFactory().get();
        String prevValue = client.set(getTableName(), id, getMapper().mapToString(entity));

        return prevValue != null ? getMapper().mapToObject(prevValue) : null;
    }
}
