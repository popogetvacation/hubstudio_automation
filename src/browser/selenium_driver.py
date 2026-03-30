"""
Selenium 驱动封装
通过 HubStudio 浏览器的 debug 端口连接
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException
)
from typing import Optional, Dict, Any, Union
from ..utils.logger import default_logger as logger

# 尝试导入 webdriver_manager
try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False


class HubStudioSeleniumDriver:
    """
    HubStudio Selenium 驱动封装

    通过 Chrome DevTools Protocol 端口连接到 HubStudio 浏览器
    """

    def __init__(self, debug_port: int,
                 chromedriver_path: str = None,
                 page_load_timeout: int = 30,
                 script_timeout: int = 30,
                 implicit_wait: int = 10):
        """
        初始化驱动

        Args:
            debug_port: Chrome DevTools Protocol 端口
            chromedriver_path: ChromeDriver 路径，优先使用 HubStudio 自带的
            page_load_timeout: 页面加载超时时间
            script_timeout: 脚本执行超时时间
            implicit_wait: 隐式等待时间
        """
        self.debug_port = debug_port
        self.chromedriver_path = chromedriver_path
        self.page_load_timeout = page_load_timeout
        self.script_timeout = script_timeout
        self.implicit_wait = implicit_wait
        self._driver: Optional[WebDriver] = None

    @property
    def default_chromedriver_path(self) -> str:
        """默认 ChromeDriver 路径（配置文件中的）"""
        return r"D:\Program Files (x86)\chromedriver.exe"

    def connect(self) -> WebDriver:
        """
        连接到 HubStudio 浏览器

        Returns:
            WebDriver 实例
        """
        if self._driver is not None:
            return self._driver

        options = Options()
        options.add_experimental_option(
            "debuggerAddress",
            f"127.0.0.1:{self.debug_port}"
        )

        try:
            # 使用指定的 ChromeDriver 路径，如果没有则使用默认路径
            driver_path = self.chromedriver_path or self.default_chromedriver_path
            logger.info(f"使用 ChromeDriver: {driver_path}")
            service = Service(executable_path=driver_path)
            self._driver = webdriver.Chrome(service=service, options=options)

            # 设置超时时间
            self._driver.set_page_load_timeout(self.page_load_timeout)
            self._driver.set_script_timeout(self.script_timeout)
            self._driver.implicitly_wait(self.implicit_wait)

            logger.info(f"成功连接到浏览器, debug_port={self.debug_port}")
            return self._driver

        except WebDriverException as e:
            logger.error(f"连接浏览器失败: {e}")
            raise

    @property
    def driver(self) -> WebDriver:
        """获取 WebDriver 实例"""
        if self._driver is None:
            raise RuntimeError("请先调用 connect() 连接浏览器")
        return self._driver

    def disconnect(self):
        """断开与浏览器的连接（不关闭浏览器）"""
        if self._driver:
            try:
                # 只断开连接，不退出浏览器
                self._driver.quit()
                logger.info("已断开与浏览器的连接")
            except Exception as e:
                logger.warning(f"断开浏览器连接时出错: {e}")
            finally:
                self._driver = None

    # ==================== 页面操作封装 ====================

    def goto(self, url: str, wait_time: int = 10) -> bool:
        """
        导航到指定 URL

        Args:
            url: 目标 URL
            wait_time: 等待时间

        Returns:
            是否成功
        """
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            logger.info(f"成功访问: {url}")
            return True
        except TimeoutException:
            logger.warning(f"页面加载超时: {url}")
            return False
        except Exception as e:
            logger.error(f"访问页面失败: {url}, 错误: {e}")
            return False

    def find_element(self, selector: str, by: str = "css",
                     timeout: int = 10):
        """
        查找元素

        Args:
            selector: 选择器
            by: 定位方式 (css, xpath, id, name, class, tag)
            timeout: 超时时间

        Returns:
            WebElement 或 None
        """
        from selenium.webdriver.common.by import By

        by_map = {
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'id': By.ID,
            'name': By.NAME,
            'class': By.CLASS_NAME,
            'tag': By.TAG_NAME
        }

        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by_map.get(by, By.CSS_SELECTOR), selector))
            )
            return element
        except TimeoutException:
            logger.warning(f"未找到元素: {selector}")
            return None

    def click(self, selector: str, by: str = "css",
              timeout: int = 10) -> bool:
        """
        点击元素

        Args:
            selector: 选择器
            by: 定位方式
            timeout: 超时时间

        Returns:
            是否成功
        """
        element = self.find_element(selector, by, timeout)
        if element:
            try:
                element.click()
                return True
            except Exception as e:
                logger.error(f"点击元素失败: {selector}, 错误: {e}")
                return False
        return False

    def input_text(self, selector: str, text: str, by: str = "css",
                   timeout: int = 10, clear_first: bool = True) -> bool:
        """
        输入文本

        Args:
            selector: 选择器
            text: 要输入的文本
            by: 定位方式
            timeout: 超时时间
            clear_first: 是否先清空

        Returns:
            是否成功
        """
        element = self.find_element(selector, by, timeout)
        if element:
            try:
                if clear_first:
                    element.clear()
                element.send_keys(text)
                return True
            except Exception as e:
                logger.error(f"输入文本失败: {selector}, 错误: {e}")
                return False
        return False

    def get_text(self, selector: str, by: str = "css",
                 timeout: int = 10) -> Optional[str]:
        """
        获取元素文本

        Args:
            selector: 选择器
            by: 定位方式
            timeout: 超时时间

        Returns:
            元素文本或 None
        """
        element = self.find_element(selector, by, timeout)
        if element:
            return element.text
        return None

    def get_attribute(self, selector: str, attribute: str,
                      by: str = "css", timeout: int = 10) -> Optional[str]:
        """
        获取元素属性

        Args:
            selector: 选择器
            attribute: 属性名
            by: 定位方式
            timeout: 超时时间

        Returns:
            属性值或 None
        """
        element = self.find_element(selector, by, timeout)
        if element:
            return element.get_attribute(attribute)
        return None

    def execute_script(self, script: str, *args):
        """
        执行 JavaScript

        Args:
            script: JavaScript 代码
            *args: 参数

        Returns:
            执行结果
        """
        return self.driver.execute_script(script, *args)

    def execute_async_script(self, script: str, *args):
        """
        执行异步 JavaScript

        Args:
            script: JavaScript 代码
            *args: 参数

        Returns:
            执行结果
        """
        return self.driver.execute_async_script(script, *args)

    def get_cookies(self) -> list:
        """获取所有 Cookies"""
        return self.driver.get_cookies()

    def add_cookie(self, cookie: Dict):
        """添加 Cookie"""
        self.driver.add_cookie(cookie)

    def get_current_url(self) -> str:
        """获取当前 URL"""
        return self.driver.current_url

    def get_page_source(self) -> str:
        """获取页面源码"""
        return self.driver.page_source

    def screenshot(self, file_path: str) -> bool:
        """
        截图

        Args:
            file_path: 截图保存路径

        Returns:
            是否成功
        """
        try:
            self.driver.save_screenshot(file_path)
            return True
        except Exception as e:
            logger.error(f"截图失败: {e}")
            return False

    def wait_for_element_visible(self, selector: str, by: str = "css",
                                  timeout: int = 10):
        """
        等待元素可见

        Args:
            selector: 选择器
            by: 定位方式
            timeout: 超时时间

        Returns:
            WebElement 或 None
        """
        from selenium.webdriver.common.by import By

        by_map = {
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'id': By.ID,
        }

        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located(
                    (by_map.get(by, By.CSS_SELECTOR), selector)
                )
            )
            return element
        except TimeoutException:
            return None

    def scroll_to_bottom(self):
        """滚动到页面底部"""
        self.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def scroll_to_element(self, selector: str, by: str = "css"):
        """滚动到指定元素"""
        element = self.find_element(selector, by)
        if element:
            self.execute_script("arguments[0].scrollIntoView(true);", element)

    def switch_to_frame(self, frame_reference):
        """切换到 iframe"""
        self.driver.switch_to.frame(frame_reference)

    def switch_to_default_content(self):
        """切换回主文档"""
        self.driver.switch_to.default_content()

    def get_new_window_handle(self) -> str:
        """获取新窗口句柄"""
        handles = self.driver.window_handles
        return handles[-1] if handles else ""

    def switch_to_window(self, window_handle: str):
        """切换到指定窗口"""
        self.driver.switch_to.window(window_handle)

    def close_current_window(self):
        """关闭当前窗口"""
        self.driver.close()

    # ==================== CDP (Chrome DevTools Protocol) ====================

    def execute_cdp_cmd(self, cmd: str, params: Dict = None) -> Dict:
        """
        执行 CDP 命令

        Args:
            cmd: CDP 命令名称
            params: 命令参数

        Returns:
            命令返回结果
        """
        return self.driver.execute_cdp_cmd(cmd, params or {})

    def get_cdp_logs(self, log_type: str = 'performance') -> List[Dict]:
        """
        获取 CDP 日志

        Args:
            log_type: 日志类型 (performance, browser, etc.)

        Returns:
            日志列表
        """
        return self.driver.get_log(log_type)
