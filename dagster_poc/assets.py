import requests
from dagster import asset, AssetIn, MetadataValue, Output, Definitions
import pandas as pd
import ramda as R
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
from io import BytesIO
import base64


@asset
def top_stories_ids():
    response = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json")
    return response.json()[:10]


@asset(ins={"ids": AssetIn("top_stories_ids")})
def top_stories(ids):
    results = R.pipe(
        R.map(
            lambda sid: requests.get(
                f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
            )
        ),
        R.map(R.invoker(0, "json")),
        pd.DataFrame,
    )

    metadata = R.apply_spec(
        {
            "num_records": len,
            "preview": R.pipe(
                R.invoker(0, "head"), R.invoker(0, "to_markdown"), MetadataValue.md
            ),
        }
    )

    return Output(results(ids), metadata=metadata(results(ids)))


@asset(ins={"stories": AssetIn("top_stories")})
def topstories_word_cloud(stories):
    stopwords = set(STOPWORDS)
    stopwords.update(["Ask", "Show", "HN", "S"])
    titles_text = " ".join([str(item) for item in stories["title"]])
    wordcloud = WordCloud(stopwords=stopwords, background_color="white").generate(
        titles_text
    )

    # Generate wordloud image
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    # Save image to buffer
    buf = BytesIO()
    plt.savefig(buf, format="png")
    image_data = base64.b64encode(buf.getvalue())

    md_content = f" ![img](data:image/png;base64,{image_data.decode()})"

    return Output(value=image_data, metadata={"plot": MetadataValue.md(md_content)})


# This is needed to deploy these assets in production.
definitions = Definitions([top_stories_ids, top_stories, topstories_word_cloud])
