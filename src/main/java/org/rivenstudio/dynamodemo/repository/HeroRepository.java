package org.rivenstudio.dynamodemo.repository;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import org.rivenstudio.dynamodemo.entity.Hero;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hero repository, CRUD operation of table Hero
 *
 * @author Riven
 */
@Repository
public class HeroRepository {
    @Autowired
    private DynamoDBMapper dynamoDBMapper;

    public Hero save(Hero hero) {
        dynamoDBMapper.save(hero);
        return hero;
    }

    public Hero getHeroById(int heroId) {
        return dynamoDBMapper.load(Hero.class, heroId);
    }

    public void delete(int heroId) {
        Hero hero = dynamoDBMapper.load(Hero.class, heroId);
        dynamoDBMapper.delete(hero);
    }

    public Hero update(int heroId, Hero hero) {
        dynamoDBMapper.save(hero,
                new DynamoDBSaveExpression()
                        .withExpectedEntry("id",
                                new ExpectedAttributeValue(
                                        new AttributeValue().withN(String.valueOf(heroId))
                                )));
        return hero;
    }

    /**
     * List Table Hero
     *
     * @return Hero list
     */
    public List<Hero> findAll() {
        return dynamoDBMapper.scan(Hero.class, new DynamoDBScanExpression());
    }

    /**
     * Filter Heroes via Name
     *
     * @param name Name term from search bar
     * @return Filtered Hero list
     */
    public List<Hero> findHeroByName(String name) {
        // Expression Attribute Names
        Map<String, String> ean = new HashMap<>();
        ean.put("#atr_name", "name");

        // Expression Attribute Values
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(name));

        return dynamoDBMapper.scan(Hero.class,
                new DynamoDBScanExpression()
                        .withFilterExpression("contains(#atr_name, :val1)")
                        .withExpressionAttributeNames(ean)
                        .withExpressionAttributeValues(eav)
        );
    }
}
