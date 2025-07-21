import requests
from bs4 import BeautifulSoup

def scrape_products(page_url):
    # 1. Fetch the HTML
    response = requests.get(page_url)
    response.raise_for_status()
    html = response.text

    # 2. Parse with BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # 3. Locate each product card
    cards = soup.select("div.group.flex.h-fit")

    products = []
    for card in cards:
        # 4a. Link and image
        link_tag = card.select_one("a[data-discover][href]")
        url = link_tag["href"]
        img_tag = card.select_one("img[alt='product']")
        img_src = img_tag["src"]

        # 4b. Discount badge
        discount_tag = card.select_one("div.bg-red-500")
        discount = discount_tag.get_text(strip=True) if discount_tag else None

        # 4c. Name and description
        name = card.select_one("h3.product-name").get_text(strip=True)
        desc = card.select_one("p.line-clamp-2.truncate").get_text(strip=True)

        # 4d. Prices
        current_price = card.select_one("p.text-primary").get_text(strip=True)
        original_price_tag = card.select_one("p.font-semibold.text-gray-500")
        original_price = original_price_tag.get_text(strip=True) if original_price_tag else None

        products.append({
            "url": url,
            "image": img_src,
            "discount": discount,
            "name": name,
            "description": desc,
            "current_price": current_price,
            "original_price": original_price,
        })

    return products

if __name__ == "__main__":
    url = "https://nomin.mn/t/131768"  # replace with the actual page URL
    for prod in scrape_products(url):
        print(prod)
