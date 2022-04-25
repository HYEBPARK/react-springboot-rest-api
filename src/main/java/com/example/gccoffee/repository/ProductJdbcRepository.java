package com.example.gccoffee.repository;

import com.example.gccoffee.JdbcUtils;
import com.example.gccoffee.model.Category;
import com.example.gccoffee.model.Product;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProductJdbcRepository implements
    ProductRepository {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public ProductJdbcRepository(
        NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static final RowMapper<Product> productRowMapper = (rs, i) -> {
        var productId = JdbcUtils.toUUID(rs.getBytes("product_id"));
        var productName = rs.getString("product_name");
        var category = Category.valueOf(rs.getString("category"));
        var price = rs.getLong("price");
        var description = rs.getString("description");
        var createdAt = JdbcUtils.toLocalDateTime(rs.getTimestamp("created_at"));
        var updatedAt = JdbcUtils.toLocalDateTime(rs.getTimestamp("updated_at"));

        return new Product(productId, productName, category, price, description, createdAt,
            updatedAt);
    };

    private Map<String, Object> toParamMap(Product product) {
        var paramMap = new HashMap<String, Object>();
        paramMap.put("productId", product.getProductId().toString().getBytes());
        paramMap.put("productName", product.getProductName());
        paramMap.put("category", product.getCategory().toString());
        paramMap.put("price", product.getPrice());
        paramMap.put("description", product.getDescription());
        paramMap.put("createdAt", product.getCreatedAt());
        paramMap.put("updatedAt", product.getUpdatedAt());
        return paramMap;
    }

    @Override
    public List<Product> findAll() {
        return namedParameterJdbcTemplate.query("select * from products", productRowMapper);
    }

    @Override
    public Product insert(Product product) {
        var update = namedParameterJdbcTemplate.update(
            "INSERT INTO products(product_id, product_name, category, price, description, created_at, updated_at) VALUES(UUID_TO_BIN(:productId), :productName, :category, :price, :description, :createdAt, :updatedAt)", toParamMap(product));

        if (update != 1) {
            throw new RuntimeException("Nothing was inserted");
        }
        return product;
    }

    @Override
    public Product update(Product product) {
        var update = namedParameterJdbcTemplate.update("UPDATE products SET product_name = :productName, category = :category, price = :price, description = :description, created_at = :createdAt, updated_at = :updatedAt WHERE product_id = UUID_TO_BIN(:productId)",toParamMap(product));

        if (update != 1) {
            throw new RuntimeException("Nothing was updated");
        }

        return product;
    }

    @Override
    public Optional<Product> findById(UUID productId) {
        try {
            return Optional.ofNullable(namedParameterJdbcTemplate.queryForObject(
                "SELECT * FROM products WHERE product_id = UUID_TO_BIN(:productId)",
                Collections.singletonMap("productId", productId.toString().getBytes()),
                productRowMapper));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Product> findByName(String productName) {
        try {
            return Optional.ofNullable(namedParameterJdbcTemplate.queryForObject(
                "SELECT * FROM products WHERE product_name = :productName",
                Collections.singletonMap("productName", productName),
                productRowMapper));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Product> findByCategory(Category category) {
        return namedParameterJdbcTemplate.query("SELECT * FROM products WHERE category = :category",
            Collections.singletonMap("category", category.toString()),
            productRowMapper);
    }

    @Override
    public void deleteAll() {
        namedParameterJdbcTemplate.update("DELETE FROM products", Collections.emptyMap());
    }
}
