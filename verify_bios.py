import asyncio
from playwright.async_api import async_playwright
import os

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # Load local widget.html
        cwd = os.getcwd()
        filepath = f"file://{cwd}/widget.html"
        await page.goto(filepath)

        # Set FTP to 230
        print("Setting FTP to 230...")
        await page.fill('#in-ftp', '230')
        await page.dispatch_event('#in-ftp', 'input')
        await page.wait_for_timeout(500) # Wait for debounce

        # Configure Mafómedes (Sector 1, 2nd tab)
        print("Configuring Mafómedes...")
        await page.click('#tabs-container .sector-tab:nth-child(2)')
        await page.wait_for_timeout(200)

        # Set speed to 15 km/h to trigger IF > 1.0
        await page.fill('#inp-speed', '15')
        await page.dispatch_event('#inp-speed', 'input')
        await page.wait_for_timeout(500)

        # Verify IF > 1.0
        if_text = await page.inner_text('#m-if')
        print(f"Mafómedes IF: {if_text}")
        if float(if_text) <= 1.0:
            print("WARNING: Mafómedes IF is not > 1.0. Increasing speed...")

        # Configure Cravelas (Sector 3, 4th tab)
        print("Configuring Cravelas...")
        await page.click('#tabs-container .sector-tab:nth-child(4)')
        await page.wait_for_timeout(200)

        # Set speed to 15 km/h to trigger IF > 1.0
        await page.fill('#inp-speed', '15')
        await page.dispatch_event('#inp-speed', 'input')
        await page.wait_for_timeout(500)

        if_text = await page.inner_text('#m-if')
        print(f"Cravelas IF: {if_text}")

        # Wait for global calculation update
        await page.wait_for_timeout(1000)

        # Get MIND value
        mind_text = await page.inner_text('#g-mind')
        print(f"Final MIND Value: {mind_text}")

        # Screenshot
        await page.screenshot(path='bios_test_result.png', full_page=True)
        print("Screenshot saved to bios_test_result.png")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
