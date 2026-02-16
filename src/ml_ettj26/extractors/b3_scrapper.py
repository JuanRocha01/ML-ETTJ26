from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError


@dataclass(frozen=True)
class B3PregaoConfig:
    page_url: str = (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/"
    )
    headless: bool = True
    timeout_ms: int = 45_000


class B3PregaoDownloader:
    """
    Downloader via UI (Playwright) para a página Pesquisa por Pregão da B3.
    """

    def __init__(self, cfg: B3PregaoConfig):
        self.cfg = cfg

    def download_zip(
        self,
        trading_date: date,
        row_unique_text: str,
        out_dir: Path,
        out_name: Optional[str] = None,
    ) -> Path:
        """
        row_unique_text: um texto que exista dentro do <tr> do arquivo (ex: "BVBG.086.01 PriceReport")
                        ou (ex: "Mercado de Derivativos - Taxas de Mercado para Swaps")
        """
        out_dir.mkdir(parents=True, exist_ok=True)

        ymd = trading_date.strftime("%Y%m%d")
        out_path = out_dir / (out_name or f"b3_{self._slug(row_unique_text)}_{ymd}.zip")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.cfg.headless)
            ctx = browser.new_context(accept_downloads=True)
            page = ctx.new_page()

            from urllib.parse import urlparse, parse_qs

            def is_probable_download_response(resp) -> bool:
                try:
                    ct = (resp.headers.get("content-type") or "").lower()
                    cd = (resp.headers.get("content-disposition") or "").lower()
                    url = resp.url.lower()
                    # heurísticas comuns: attachment, zip, octet-stream, fileDownload.jsp
                    if "attachment" in cd:
                        return True
                    if "application/zip" in ct or "octet-stream" in ct:
                        return True
                    if "filedownload.jsp" in url or "filedownload" in url:
                        return True
                    return False
                except Exception:
                    return False

            def print_req(req):
                url = req.url.lower()
                if ("pesquisapregao" in url) or ("filelist=" in url) or ("filedownload" in url) or ("download" in url):
                    parsed = urlparse(req.url)
                    qs = parse_qs(parsed.query)
                    print("\n[REQUEST]")
                    print(req.method, req.url)
                    if qs:
                        print("query:", {k: v[:3] for k, v in qs.items()})


            def print_resp(resp):
                if is_probable_download_response(resp):
                    url = resp.url
                    parsed = urlparse(url)
                    qs = parse_qs(parsed.query)
                    print("\n[DOWNLOAD RESPONSE DETECTED]")
                    print("status:", resp.status)
                    print("url:", url)
                    print("content-type:", resp.headers.get("content-type"))
                    print("content-disposition:", resp.headers.get("content-disposition"))
                    if qs:
                        print("query:", {k: v[:3] for k, v in qs.items()})
                    if "pesquisapregao" in url:
                        return True


            page.on("request", print_req)
            page.on("response", print_resp)


            page.set_default_timeout(self.cfg.timeout_ms)

            page.goto(self.cfg.page_url, wait_until="domcontentloaded")

            # (0) limpar seleção anterior (se existir)
            self._try_clear(page)

            # (1) localizar a linha
            row = page.locator("tr", has=page.get_by_text(row_unique_text, exact=False)).first
            if row.count() == 0:
                raise RuntimeError(f"Não achei linha contendo: {row_unique_text}")

            # (2) preencher data PRIMEIRO (datepicker costuma validar antes de permitir seleção)
            ds = trading_date.strftime("%d/%m/%Y")
            date_input = row.locator("input.datepicker, input.hasDatepicker").first
            if date_input.count() == 0:
                raise RuntimeError("Não achei input de data (.datepicker) dentro do <tr>.")

            self._fill_datepicker(page, date_input, ds)

            # (3) marcar checkbox DEPOIS, usando click + is_checked (mais tolerante a JS que desfaz)
            checkbox = row.locator("input[type='checkbox']").first
            if checkbox.count() == 0:
                raise RuntimeError("Linha encontrada, mas não achei checkbox dentro do <tr>.")

            self._click_checkbox_until_checked(page, checkbox, row)

            # se mesmo assim não marcou, provavelmente não existe arquivo para a data
            if not checkbox.is_checked():
                msg = self._read_error(page) or (
                    "Não foi possível selecionar o arquivo (provável indisponibilidade do Swap para a data)."
                )
                raise RuntimeError(msg)

            # (4) download
            self._click_download_and_save(page, out_path)

            ctx.close()
            browser.close()

        if not out_path.exists() or out_path.stat().st_size == 0:
            raise RuntimeError(f"Download vazio/ausente: {out_path}")

        return out_path

    def _click_download_and_save(self, page, out_path: Path) -> None:
        download_btn = page.locator("#botao-download")

        # Se a UI estiver reclamando de seleção, falha cedo
        err = self._read_error(page)
        if err:
            raise RuntimeError(err)

        try:
            with page.expect_download(timeout=self.cfg.timeout_ms) as dl_info:
                download_btn.click()
            dl = dl_info.value
            dl.save_as(str(out_path))
            return

        except PwTimeoutError:
            # fallback: às vezes precisa de um pequeno delay (datepicker/validação)
            page.wait_for_timeout(800)

            err = self._read_error(page)
            if err:
                raise RuntimeError(err)

            try:
                with page.expect_download(timeout=10_000) as dl_info2:
                    download_btn.click()
                dl2 = dl_info2.value
                dl2.save_as(str(out_path))
                return
            except PwTimeoutError:
                raise RuntimeError("Não houve evento de download (timeout). Pode ser que não exista arquivo para a data.")

    def _try_clear(self, page) -> None:
        # botão LIMPAR existe: <input type="button" ... value="LIMPAR">
        try:
            clear_btn = page.get_by_role("button", name="LIMPAR")
            if clear_btn.count() and clear_btn.first.is_visible():
                clear_btn.first.click()
                return
        except Exception:
            pass

        try:
            page.get_by_text("LIMPAR", exact=False).click()
        except Exception:
            pass

    def _read_error(self, page) -> Optional[str]:
        try:
            err = page.locator("small.error").first
            if err.count() and err.is_visible():
                txt = err.inner_text().strip()
                return txt if txt else None
        except Exception:
            pass
        return None

    def _slug(self, s: str) -> str:
        return (
            s.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "")
            .replace("/", "_")
        )
    
    def _fill_datepicker(self, page, date_input, ds: str) -> None:
        # força foco, preenche, dispara eventos e blur
        date_input.scroll_into_view_if_needed()
        date_input.click(force=True)
        date_input.fill(ds)

        # muitos datepickers só “aceitam” no change/blur
        try:
            date_input.dispatch_event("input")
            date_input.dispatch_event("change")
        except Exception:
            pass

        # blur
        page.keyboard.press("Tab")
        page.wait_for_timeout(150)


    def _click_checkbox_until_checked(self, page, checkbox, row) -> None:
        checkbox.scroll_into_view_if_needed()

        # 1) tenta clicar no próprio input
        checkbox.click(force=True)
        page.wait_for_timeout(150)
        if checkbox.is_checked():
            return

        # 2) fallback: clicar na célula do checkbox (às vezes o handler está no td/label)
        try:
            row.locator("td").first.click(force=True)
            page.wait_for_timeout(150)
            if checkbox.is_checked():
                return
        except Exception:
            pass

        # 3) fallback: clicar no label associado ao checkbox (se existir)
        try:
            cid = checkbox.get_attribute("id")
            if cid:
                lbl = page.locator(f"label[for='{cid}']")
                if lbl.count() and lbl.first.is_visible():
                    lbl.first.click(force=True)
                    page.wait_for_timeout(150)
        except Exception:
            pass

#%%
from datetime import date
from pathlib import Path
def main_0():
    downloader = B3PregaoDownloader(B3PregaoConfig(headless=False))

    PRICE_REPORT = "BVBG.086.01 PriceReport"
    SWAP_RATES = "Mercado de Derivativos - Taxas de Mercado para Swaps"
    
    out = Path("data/b3/pregao")

    # PriceReport (exemplo)
    downloader.download_zip(date(2021, 3, 2), PRICE_REPORT, out)

    # Swap Market Rates (exemplo)
    #downloader.download_zip(date(2020, 2, 13), SWAP_RATES, out)

from datetime import date
from pathlib import Path
from playwright.sync_api import sync_playwright

def main():
    page_url = (
        "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/"
        "market-data/historico/boletins-diarios/pesquisa-por-pregao/pesquisa-por-pregao/"
    )

    out = Path("data/b3/pregao")
    out.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(accept_downloads=True)
        page = ctx.new_page()
        page.set_default_timeout(45_000)
        page.goto(page_url, wait_until="domcontentloaded")

        SWAP_CONTENT_ID = "8AA8D0975E207D10015E28F8EFBF5199"
        DOWNLOAD_TOKEN = "8A68812D566A8C3D01566AA81FA94EE3"

        d = date(2020, 2, 13)
        ds = d.strftime("%d/%m/%Y")

        # Preenche data
        date_sel = f"input[id='{SWAP_CONTENT_ID}.date']"
        page.locator(date_sel).fill(ds)
        page.locator(date_sel).dispatch_event("input")
        page.locator(date_sel).dispatch_event("change")
        page.keyboard.press("Tab")

        # Marca checkbox via JS
        page.evaluate(
            """(contentId) => {
                const cb = document.getElementById(contentId);
                if (!cb) throw new Error("checkbox not found: " + contentId);
                cb.checked = true;
                cb.dispatchEvent(new Event("click", { bubbles: true }));
                cb.dispatchEvent(new Event("change", { bubbles: true }));
                return cb.checked;
            }""",
            SWAP_CONTENT_ID
        )

        # Heurística para reconhecer a response do download
        def is_attachment(resp):
            try:
                cd = (resp.headers.get("content-disposition") or "").lower()
                ct = (resp.headers.get("content-type") or "").lower()
                url = resp.url.lower()
                return (
                    ("attachment" in cd)
                    or ("octet-stream" in ct)
                    or ("application/zip" in ct)
                    or ("pesquisapregao" in url and "download" in url)
                )
            except Exception:
                return False

        out_path = out / "swap_20200213.zip"

        # Espera a response binária nascer quando chamarmos downloadFiles(...)
        with page.expect_response(lambda r: is_attachment(r), timeout=45_000) as resp_info:
            page.evaluate("""(token) => { downloadFiles(token); }""", DOWNLOAD_TOKEN)

        resp = resp_info.value
        print("DOWNLOAD URL:", resp.url)
        print("CD:", resp.headers.get("content-disposition"))
        print("CT:", resp.headers.get("content-type"))

        out_path.write_bytes(resp.body())
        print("saved:", out_path)

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()

