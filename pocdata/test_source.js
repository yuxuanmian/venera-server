class TestSource extends ComicSource {
    name = "TestSource"
    key = "test_source"
    version = "1.0.0"
    url = ""

    comic = {
        loadInfo: async (id) => {
            let res = await Network.get(`https://example.com/comic/${id}`);
            if (res.status !== 200) {
                throw new Error(`HTTP ${res.status}`);
            }
            let json = JSON.parse(res.body);
            return {
                id: id,
                title: json.title,
                cover: json.cover,
                updateTime: json.updateTime,
                chapters: json.chapters,
            };
        }
    }
}
