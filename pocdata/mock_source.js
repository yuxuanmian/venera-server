class MockSource extends ComicSource {
    name = "MockSource"
    key = "mock_source"
    version = "1.0.0"
    minAppVersion = "0.0.0"
    url = "http://127.0.0.1:8765"
    search = {}

    comic = {
        loadInfo: async (id) => {
            let res = await Network.get(`http://127.0.0.1:8765/comic/${id}`);
            if (res.status !== 200) {
                throw new Error(`HTTP ${res.status}`);
            }
            let json = JSON.parse(res.body);
            return {
                id: id,
                title: json.title,
                cover: json.cover,
                updateTime: json.updateTime,
                tags: {},
                chapters: json.chapters,
            };
        }
    }
}
