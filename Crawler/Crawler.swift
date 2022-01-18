//
//  Crawler.swift
//  Crawler
//
//  Created by Chris Eidhof on 21.12.21.
//

import Foundation

actor Queue {
    var items: Set<URL> = []
    var inProgress: Set<URL> = []
    var seen: Set<URL> = []
    var pendingDequeues: [() -> ()] = []
    
    func dequeue() async -> URL? {
        if let result = items.popFirst() {
            inProgress.insert(result)
            return result
        } else {
            if done {
                return nil
            }
            print("No items, going to suspend...")
            await withCheckedContinuation { cont in
                pendingDequeues.append(cont.resume)
            }
            return await dequeue()
        }
    }
    
    func finish(_ item: URL) {
        inProgress.remove(item)
        if done {
            flushPendingDequeues()
        }
    }
    
    private var done: Bool {
        items.isEmpty && inProgress.isEmpty
    }
    
    func add(newItems: [URL]) {
        let trulyNew = newItems.filter { !seen.contains($0) }
        seen.formUnion(trulyNew)
        items.formUnion(trulyNew)
        flushPendingDequeues()
    }
    
    private func flushPendingDequeues() {
        for cont in pendingDequeues {
            cont()
        }
        pendingDequeues.removeAll()
    }
}


typealias CrawlerStream = AsyncThrowingStream<Page, Error>

fileprivate func crawlHelper(url: URL, numberOfWorkers: Int, cont: CrawlerStream.Continuation) async throws {
    let basePrefix = url.absoluteString
    let queue = Queue()
    await queue.add(newItems: [url])
    try await withThrowingTaskGroup(of: Void.self) { group in
        for i in 0..<numberOfWorkers {
            group.addTask {
                var numberOfJobs = 0
                while let job = await queue.dequeue() {
                    let page = try await URLSession.shared.page(from: job)
                    let newURLs = page.outgoingLinks.filter { url in
                        url.absoluteString.hasPrefix(basePrefix)
                    }
                    await queue.add(newItems: newURLs)
                    cont.yield(page)
                    await queue.finish(page.url)
                    numberOfJobs += 1
                }
                print("Worker \(i) did \(numberOfJobs) jobs")
            }
        }
        for try await _ in group {
        }
    }
}

func crawl(url: URL, numberOfWorkers: Int = 4) -> CrawlerStream {
    return CrawlerStream { cont in
        Task {
            do {
                try await crawlHelper(url: url, numberOfWorkers: numberOfWorkers, cont: cont)
                cont.finish(throwing: nil)
            } catch {
                cont.finish(throwing: error)
            }
        }
    }
}

extension URLSession {
    func page(from url: URL) async throws -> Page {
        let (data, _) = try await data(from: url)
        let doc = try XMLDocument(data: data, options: .documentTidyHTML)
        let title = try doc.nodes(forXPath: "//title").first?.stringValue
        let links: [URL] = try doc.nodes(forXPath: "//a[@href]").compactMap { node in
            guard let el = node as? XMLElement else { return nil }
            guard let href = el.attribute(forName: "href")?.stringValue else { return nil }
            return URL(string: href, relativeTo: url)?.simplified
        }
        return Page(url: url, title: title ?? "", outgoingLinks: links)
    }
}

extension URL {
    var simplified: URL {
        var result = absoluteString
        if let i = result.lastIndex(of: "#") {
            result = String(result[..<i])
        }
        if result.last == "/" {
            result.removeLast()
        }
        return URL(string: result)!
    }
}

extension URL: @unchecked Sendable { }

struct Page {
    var url: URL
    var title: String
    var outgoingLinks: [URL]
}
