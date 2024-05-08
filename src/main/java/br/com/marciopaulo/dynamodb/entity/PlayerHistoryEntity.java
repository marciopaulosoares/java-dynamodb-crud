package br.com.marciopaulo.dynamodb.entity;

import br.com.marciopaulo.dynamodb.dto.ScoreDto;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.time.Instant;
import java.util.UUID;

@DynamoDbBean
public class PlayerHistoryEntity {
    private String username;
    private UUID gameId;
    private Double score;
    private Instant createdAt;

    public  static PlayerHistoryEntity fromScore(String username, ScoreDto scoreDto){
        var entity = new PlayerHistoryEntity();
        entity.setUsername(username);
        entity.setGameId(UUID.randomUUID());
        entity.setScore(scoreDto.score());
        entity.setCreatedAt(Instant.now());
        return entity;
    }

    @DynamoDbAttribute("username")
    @DynamoDbPartitionKey
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @DynamoDbAttribute("game_id")
    @DynamoDbSortKey
    public UUID getGameId() {
        return gameId;
    }

    public void setGameId(UUID gameId) {
        this.gameId = gameId;
    }

    @DynamoDbAttribute("score")
    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @DynamoDbAttribute("created_at")
    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}
