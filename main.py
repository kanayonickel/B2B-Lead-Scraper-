"""
Nigerian B2B Lead Scraper for ERP Marketing
Targets: Financial Reconciliation, Supply Chain, Invoicing
Legal - Uses only publicly available data
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
import asyncio
import json
import re
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from datetime import datetime
from typing import List
import csv
import random

app = FastAPI()
active_connections: List[WebSocket] = []


class NigerianLeadScraper:
    def __init__(self):
        self.leads = []
        self.industries = [
            "manufacturing companies in Nigeria",
            "logistics companies in Nigeria",
            "import export companies in Nigeria",
            "wholesale distributors in Nigeria",
            "retail chains in Nigeria",
            "trading companies in Nigeria",
            "pharmaceutical companies in Nigeria",
            "FMCG companies in Nigeria",
            "oil and gas companies in Nigeria",
            "construction companies in Nigeria"
        ]
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
    
    async def send_update(self, message: dict):
        for connection in active_connections:
            try:
                await connection.send_json(message)
            except:
                pass
    
    def extract_emails(self, text):
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        filtered = []
        excluded = ['example.com', 'domain.com', 'yourcompany.com', 'test.com', 'sample.com']
        for email in emails:
            email_lower = email.lower()
            if not any(ex in email_lower for ex in excluded):
                if any(keyword in email_lower for keyword in ['info', 'contact', 'sales', 'admin', 'support', 'business']) or \
                   not any(generic in email_lower for generic in ['noreply', 'no-reply', 'donotreply']):
                    filtered.append(email)
        return list(set(filtered))
    
    def extract_phones(self, text):
        patterns = [
            r'\+234[- ]?\d{3}[- ]?\d{3}[- ]?\d{4}',
            r'0[7-9][0-1]\d{8}',
            r'\(0\)\d{3}[- ]?\d{3}[- ]?\d{4}'
        ]
        phones = []
        for pattern in patterns:
            phones.extend(re.findall(pattern, text))
        return list(set(phones))[:3]
    
    async def search_google_maps(self, industry, max_results=30):
        await self.send_update({
            "type": "status",
            "message": f"🔍 Searching Google Maps: {industry}...",
            "color": "blue"
        })
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = random.choice(self.user_agents)
            context = await browser.new_context(user_agent=ua)
            page = await context.new_page()
            await stealth_async(page)
            
            try:
                # Add location specificity for better results
                search_query = f"{industry.replace(' ', '+')} in Lagos Nigeria"
                await page.goto(f"https://www.google.com/maps/search/{search_query}", timeout=30000)
                await page.wait_for_load_state('networkidle')
                await page.wait_for_timeout(5000)
                
                # Wait for results container
                await page.wait_for_selector('div[role="feed"]', timeout=30000)
                
                # Aggressive scrolling
                for _ in range(8):
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 1.5)")
                    await page.wait_for_timeout(random.randint(2500, 4500))
                
                # Updated selectors
                business_cards = await page.query_selector_all('a[href*="place/"]')
                company_data = []
                
                for card in business_cards[:max_results * 2]:
                    try:
                        # Click to open side panel for details
                        await card.click()
                        await page.wait_for_timeout(2000)
                        
                        # Name
                        name_elem = await page.query_selector('h1.fontHeadlineLarge, span.fontHeadlineLarge')
                        company_name = await name_elem.inner_text() if name_elem else "Unknown"
                        
                        # Website
                        website_elem = await page.query_selector('a[data-item-id="authority"]')
                        website = await website_elem.get_attribute('href') if website_elem else None
                        
                        # Phone
                        phone_elem = await page.query_selector('button[data-item-id*="phone"]')
                        phone = await phone_elem.inner_text() if phone_elem else None
                        
                        if company_name != "Unknown":
                            company_data.append({
                                "name": company_name,
                                "website": website,
                                "phone": phone
                            })
                    except:
                        continue
                
                await self.send_update({
                    "type": "status",
                    "message": f"✓ Found {len(company_data)} potential businesses from Maps",
                    "color": "green"
                })
                
                # Process each
                leads_found = 0
                for data in company_data:
                    if leads_found >= max_results:
                        break
                    
                    emails = []
                    phones = [data["phone"]] if data["phone"] else []
                    website = data["website"]
                    
                    if website:
                        try:
                            await page.goto(website, timeout=15000, wait_until='domcontentloaded')
                            await asyncio.sleep(2)
                            
                            page_text = await page.evaluate('() => document.body.innerText')
                            emails = self.extract_emails(page_text)
                            phones.extend(self.extract_phones(page_text))
                            phones = list(set(phones))[:3]
                        except:
                            pass
                    
                    if emails or phones:
                        lead = {
                            "id": len(self.leads) + 1,
                            "company": data["name"].strip()[:100],
                            "industry": industry,
                            "website": website or "Not found",
                            "emails": ', '.join(emails[:3]),
                            "phones": ', '.join(phones) if phones else "Not found",
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        self.leads.append(lead)
                        leads_found += 1
                        
                        await self.send_update({
                            "type": "lead",
                            "data": lead
                        })
                        
                        await asyncio.sleep(random.uniform(1, 3))
            
            except Exception as e:
                await self.send_update({
                    "type": "status",
                    "message": f"Error: {str(e)}",
                    "color": "red"
                })
            finally:
                await browser.close()
    
    async def scrape_nigerian_directories(self, max_results=30):
        await self.send_update({
            "type": "status",
            "message": "📋 Scraping Nigerian business directories...",
            "color": "blue"
        })
        
        directories = [
            "https://www.finelib.com",
            "https://www.businesslist.com.ng",
            "https://www.vconnect.com"
        ]
        
        # Placeholder for directory scraping - expand as needed
        await self.send_update({
            "type": "status",
            "message": "Directory scraping: Add parsers for each site",
            "color": "blue"
        })
    
    def export_to_csv(self):
        filename = f"nigerian_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            if self.leads:
                writer = csv.DictWriter(f, fieldnames=self.leads[0].keys())
                writer.writeheader()
                writer.writerows(self.leads)
        return filename


scraper = NigerianLeadScraper()


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nigerian B2B Lead Scraper</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen">
    <div class="container mx-auto px-4 py-8 max-w-7xl">
        <!-- Header -->
        <div class="mb-8">
            <h1 class="text-4xl font-bold text-slate-100 mb-2">🇳🇬 Kanayo - Nigerian B2B Lead Scraper</h1>
            <p class="text-slate-400 text-lg">Find companies for your ERP solution (Financial Reconciliation, Supply Chain, Invoicing)</p>
        </div>

        <!-- Control Panel -->
        <div class="bg-slate-800 rounded-lg shadow-lg p-6 mb-6 border border-slate-700">
            <h2 class="text-xl font-bold mb-4 text-slate-200">Search Industries</h2>
            
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 mb-4">
                <button onclick="searchIndustry('manufacturing')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🏭 Manufacturing
                </button>
                <button onclick="searchIndustry('logistics')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🚚 Logistics
                </button>
                <button onclick="searchIndustry('import-export')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    📦 Import/Export
                </button>
                <button onclick="searchIndustry('wholesale')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🏪 Wholesale
                </button>
                <button onclick="searchIndustry('retail')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🛒 Retail Chains
                </button>
                <button onclick="searchIndustry('pharmaceutical')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    💊 Pharmaceutical
                </button>
                <button onclick="searchIndustry('fmcg')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🥫 FMCG
                </button>
                <button onclick="searchIndustry('construction')" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🏗️ Construction
                </button>
                <button onclick="searchAll()" class="bg-green-700 hover:bg-green-600 text-white font-semibold py-3 px-4 rounded-lg shadow transition text-sm">
                    🎯 Search All Industries
                </button>
            </div>

            <div class="flex gap-4 mt-4">
                <button onclick="clearLeads()" class="bg-slate-700 hover:bg-slate-600 text-white font-semibold py-3 px-6 rounded-lg shadow transition">
                    🗑️ Clear Results
                </button>
                <button onclick="exportCSV()" class="bg-blue-700 hover:bg-blue-600 text-white font-semibold py-3 px-6 rounded-lg shadow transition">
                    📥 Export to CSV
                </button>
            </div>
        </div>

        <!-- Status Log -->
        <div class="bg-slate-800 rounded-lg shadow-lg p-6 mb-6 border border-slate-700">
            <h2 class="text-xl font-bold mb-4 text-green-400">📊 Status Log</h2>
            <div id="statusLog" class="space-y-2 max-h-40 overflow-y-auto bg-slate-900 rounded-lg p-4 border border-slate-700">
                <p class="text-slate-400">Waiting to start scraping...</p>
            </div>
        </div>

        <!-- Leads Table -->
        <div class="bg-slate-800 rounded-lg shadow-lg p-6 border border-slate-700">
            <div class="flex justify-between items-center mb-4">
                <h2 class="text-xl font-bold text-slate-200">📋 Leads Found</h2>
                <span id="leadCount" class="text-slate-400 text-sm">0 leads</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead>
                        <tr class="border-b border-slate-700">
                            <th class="py-3 px-4 text-slate-400 font-semibold">#</th>
                            <th class="py-3 px-4 text-slate-400 font-semibold">Company</th>
                            <th class="py-3 px-4 text-slate-400 font-semibold">Industry</th>
                            <th class="py-3 px-4 text-slate-400 font-semibold">Emails</th>
                            <th class="py-3 px-4 text-slate-400 font-semibold">Phones</th>
                            <th class="py-3 px-4 text-slate-400 font-semibold">Website</th>
                        </tr>
                    </thead>
                    <tbody id="leadsTable">
                        <tr>
                            <td colspan="6" class="text-center py-8 text-slate-500">
                                No leads yet. Start searching to find companies! 🚀
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        let ws = null;
        let leads = [];

        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                
                if (data.type === 'status') {
                    addStatusLog(data.message, data.color);
                } else if (data.type === 'lead') {
                    addLead(data.data);
                }
            };
            
            ws.onclose = function() {
                console.log('WebSocket closed. Reconnecting...');
                setTimeout(connectWebSocket, 1000);
            };
        }

        function addStatusLog(message, color = 'gray') {
            const log = document.getElementById('statusLog');
            const colorClasses = {
                'blue': 'text-blue-400',
                'green': 'text-green-400',
                'red': 'text-red-400',
                'gray': 'text-slate-400'
            };
            const p = document.createElement('p');
            p.className = `${colorClasses[color]} text-sm`;
            p.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
            log.insertBefore(p, log.firstChild);
        }

        function addLead(data) {
            leads.push(data);
            const tbody = document.getElementById('leadsTable');
            
            if (leads.length === 1) {
                tbody.innerHTML = '';
            }
            
            const row = document.createElement('tr');
            row.className = 'border-b border-slate-700 hover:bg-slate-700 transition';
            
            row.innerHTML = `
                <td class="py-3 px-4 text-slate-400">${data.id}</td>
                <td class="py-3 px-4 font-medium text-slate-200">${data.company}</td>
                <td class="py-3 px-4 text-slate-400 text-xs">${data.industry}</td>
                <td class="py-3 px-4 text-green-400 text-xs font-mono">${data.emails}</td>
                <td class="py-3 px-4 text-slate-400 text-xs">${data.phones}</td>
                <td class="py-3 px-4"><a href="${data.website}" target="_blank" class="text-blue-400 hover:text-blue-300 text-xs">🔗 Visit</a></td>
            `;
            
            tbody.insertBefore(row, tbody.firstChild);
            document.getElementById('leadCount').textContent = `${leads.length} leads`;
        }

        function searchIndustry(industry) {
            fetch(`/search/${industry}`, {method: 'POST'});
            addStatusLog(`🔍 Searching ${industry} companies...`, 'blue');
        }

        function searchAll() {
            fetch('/search/all', {method: 'POST'});
            addStatusLog('🎯 Starting comprehensive search across all industries...', 'blue');
        }

        function clearLeads() {
            leads = [];
            document.getElementById('leadsTable').innerHTML = `
                <tr>
                    <td colspan="6" class="text-center py-8 text-slate-500">
                        No leads yet. Start searching to find companies! 🚀
                    </td>
                </tr>
            `;
            document.getElementById('leadCount').textContent = '0 leads';
            addStatusLog('🗑️ Results cleared', 'gray');
        }

        function exportCSV() {
            if (leads.length === 0) {
                alert('No leads to export!');
                return;
            }
            
            window.location.href = '/export/csv';
            addStatusLog('📥 Exporting to CSV...', 'green');
        }

        connectWebSocket();
    </script>
</body>
</html>
    """


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)


@app.post("/search/{industry}")
async def search_industry(industry: str):
    industry_map = {
        "manufacturing": "manufacturing companies in Nigeria",
        "logistics": "logistics companies in Nigeria",
        "import-export": "import export companies in Nigeria",
        "wholesale": "wholesale distributors in Nigeria",
        "retail": "retail chains in Nigeria",
        "pharmaceutical": "pharmaceutical companies in Nigeria",
        "fmcg": "FMCG companies in Nigeria",
        "construction": "construction companies in Nigeria"
    }
    
    search_term = industry_map.get(industry, f"{industry} companies in Nigeria")
    asyncio.create_task(scraper.search_google_maps(search_term, max_results=30))
    return {"status": "started"}


@app.post("/search/all")
async def search_all():
    async def run_all():
        for industry in scraper.industries:
            await scraper.search_google_maps(industry, max_results=30)
    
    asyncio.create_task(run_all())
    return {"status": "started"}


@app.get("/export/csv")
async def export_csv():
    filename = scraper.export_to_csv()
    return FileResponse(filename, filename=filename, media_type='text/csv')


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)