package com.timxs.storagetoolkit.service.impl;

import com.timxs.storagetoolkit.extension.BrokenLink;
import com.timxs.storagetoolkit.extension.WhitelistEntry;
import com.timxs.storagetoolkit.service.WhitelistService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import run.halo.app.extension.ListOptions;
import run.halo.app.extension.Metadata;
import run.halo.app.extension.ReactiveExtensionClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static run.halo.app.extension.index.query.Queries.equal;

/**
 * 白名单管理服务实现
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WhitelistServiceImpl implements WhitelistService {

    private final ReactiveExtensionClient client;

    @Override
    public Flux<WhitelistItem> list() {
        return client.listAll(WhitelistEntry.class, ListOptions.builder().build(), Sort.unsorted())
            .map(this::toWhitelistItem);
    }

    @Override
    public Flux<WhitelistItem> search(String keyword) {
        return client.listAll(WhitelistEntry.class, ListOptions.builder().build(), Sort.unsorted())
            .filter(entry -> {
                if (entry.getSpec() == null) return false;
                String url = entry.getSpec().getUrl();
                String note = entry.getSpec().getNote();
                // 在 URL 或备注中搜索关键词
                if (url != null && url.toLowerCase().contains(keyword.toLowerCase())) {
                    return true;
                }
                return note != null && note.toLowerCase().contains(keyword.toLowerCase());
            })
            .map(this::toWhitelistItem);
    }

    @Override
    public Mono<Boolean> isWhitelisted(String url) {
        if (url == null || url.isBlank()) {
            return Mono.just(false);
        }

        return client.listAll(WhitelistEntry.class, ListOptions.builder().build(), Sort.unsorted())
            .filter(entry -> entry.getSpec() != null)
            .any(entry -> {
                String whitelistUrl = entry.getSpec().getUrl();
                String matchMode = entry.getSpec().getMatchMode();
                if (whitelistUrl == null) return false;

                // 精确匹配
                if ("exact".equals(matchMode)) {
                    return url.equals(whitelistUrl);
                }
                // 前缀匹配（默认）
                return url.startsWith(whitelistUrl);
            });
    }

    @Override
    public Mono<WhitelistItem> add(String url, String note, String matchMode) {
        if (url == null || url.isBlank()) {
            return Mono.error(new IllegalArgumentException("URL 不能为空"));
        }

        String resolvedMatchMode = matchMode != null ? matchMode : "exact";

        // 先查询是否已存在相同 url + matchMode 的记录
        return findExisting(url, resolvedMatchMode)
            .flatMap(existing -> {
                // 已存在，仅在有新备注时更新
                if (note != null && !note.isBlank()) {
                    existing.getSpec().setNote(note);
                    return client.update(existing);
                }
                return Mono.just(existing);
            })
            .switchIfEmpty(Mono.defer(() -> {
                // 不存在，创建新记录
                WhitelistEntry entry = new WhitelistEntry();
                Metadata metadata = new Metadata();
                metadata.setName(generateName(url));
                entry.setMetadata(metadata);

                WhitelistEntry.WhitelistEntrySpec spec = new WhitelistEntry.WhitelistEntrySpec();
                spec.setUrl(url);
                spec.setNote(note);
                spec.setCreatedAt(Instant.now());
                spec.setMatchMode(resolvedMatchMode);
                entry.setSpec(spec);

                return client.create(entry);
            }))
            .map(this::toWhitelistItem)
            // 添加白名单后，删除匹配的断链记录
            .flatMap(item -> deleteMatchingBrokenLinks(url, resolvedMatchMode)
                .thenReturn(item));
    }

    @Override
    public Flux<WhitelistItem> addBatch(List<String> urls) {
        if (urls == null || urls.isEmpty()) {
            return Flux.empty();
        }

        return Flux.fromStream(urls.stream())
            .flatMap(url -> add(url, null, "exact")
                .onErrorResume(e -> {
                    log.warn("添加白名单失败: {} - {}", url, e.getMessage());
                    return Mono.empty();
                }));
    }

    @Override
    public Mono<Void> delete(String name) {
        return client.fetch(WhitelistEntry.class, name)
            .flatMap(entry -> client.delete(entry).then(Mono.empty()));
    }

    @Override
    public Mono<Void> clearAll() {
        return client.listAll(WhitelistEntry.class, ListOptions.builder().build(), Sort.unsorted())
            .collectList()
            .flatMap(list -> {
                List<Mono<Void>> deleteMonos = new ArrayList<>();
                for (WhitelistEntry entry : list) {
                    deleteMonos.add(client.delete(entry).then(Mono.empty()));
                }
                return Mono.when(deleteMonos.toArray(new Mono[0]));
            });
    }

    private WhitelistItem toWhitelistItem(WhitelistEntry entry) {
        if (entry.getSpec() == null) {
            return null;
        }
        return new WhitelistItem(
            entry.getMetadata().getName(),
            entry.getSpec().getUrl(),
            entry.getSpec().getNote(),
            entry.getSpec().getCreatedAt(),
            entry.getSpec().getMatchMode()
        );
    }

    /**
     * 查找已存在的相同 url + matchMode 的白名单记录
     */
    private Mono<WhitelistEntry> findExisting(String url, String matchMode) {
        return client.listAll(WhitelistEntry.class,
                ListOptions.builder()
                    .fieldQuery(equal("spec.url", url))
                    .build(),
                Sort.unsorted())
            .filter(entry -> entry.getSpec() != null
                && matchMode.equals(entry.getSpec().getMatchMode()))
            .next();
    }

    /**
     * 删除与白名单 URL 匹配的断链记录
     * 匹配规则：BrokenLink 的 spec.url 或 status.originalUrl 等于白名单 URL（精确匹配），
     * 或以白名单 URL 为前缀（前缀匹配）
     */
    private Mono<Void> deleteMatchingBrokenLinks(String whitelistUrl, String matchMode) {
        return client.listAll(BrokenLink.class, ListOptions.builder().build(), Sort.unsorted())
            .filter(link -> {
                if (link.getSpec() == null) return false;
                String linkUrl = link.getSpec().getUrl();
                String originalUrl = link.getStatus() != null
                    ? link.getStatus().getOriginalUrl() : null;

                if ("exact".equals(matchMode)) {
                    return whitelistUrl.equals(linkUrl)
                        || whitelistUrl.equals(originalUrl);
                }
                // 前缀匹配
                boolean urlMatch = linkUrl != null && linkUrl.startsWith(whitelistUrl);
                boolean originalMatch = originalUrl != null
                    && originalUrl.startsWith(whitelistUrl);
                return urlMatch || originalMatch;
            })
            .flatMap(link -> client.delete(link)
                .doOnSuccess(v -> log.debug("已删除匹配的断链记录: {}", link.getSpec().getUrl()))
                .onErrorResume(e -> {
                    log.warn("删除断链记录失败: {} - {}", link.getSpec().getUrl(), e.getMessage());
                    return Mono.empty();
                }))
            .then();
    }

    /**
     * 根据 URL 生成唯一的资源名称
     * 使用时间戳和纳秒时间确保唯一性，与断链记录逻辑一致
     */
    private String generateName(String url) {
        // 使用时间戳和纳秒时间作为名称，确保唯一性
        return "whitelist-" + System.currentTimeMillis() + "-" + System.nanoTime();
    }
}
