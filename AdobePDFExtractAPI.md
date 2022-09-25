# Adobe PDF Extract API (part of Adobe PDF Services API):

### Description:
The [PDF Extract API](https://developer.adobe.com/document-services/docs/overview/pdf-extract-api/) (included with the PDF Services API) is a cloud-based web service that uses Adobe’s Sensei AI technology to automatically extract content and structural information from PDF documents – native or scanned – and to output it in a structured JSON format. The service extracts text, complex tables, and figures.

Text is extracted in contextual blocks – paragraphs, headings, lists, footnotes, etc. – and includes font, styling, and other text formatting information. The JSON output also captures document structure information, such as the natural reading order of the various extracted elements and the layout of the elements on each given page.

The PDF Extract API provides a method for developers to extract and structure content for use in a number of downstream applications including content republishing, content processing, data analysis, and content aggregation, management, and search.

### Terminlogy:
- Document Transaction: Operation over a set of pages (5 pages for PDF extract, 50 pages for other services).

### Free Trial:
- Benefits: 1000 Document Transactions, over a period of 6 months (whichever exhausts first).
- Non credit-card / payment method registration required, only one Adobe account (free to create) required.

### Pricing:
[Available at developer.above.com](https://developer.adobe.com/document-services/pricing/#main)
Plans:
- [Pay-as-you-go](https://www.adobe.com/go/pdfToolsAPI_AWS_Intl):
  - $0.05 (Rs. 4.06) per Document Transaction
  - Access to all API services.
- [Volume Pricing](https://developer.adobe.com/document-services/pricing/contact/sales/):
  - Large volume transactions.
  - Access to all API services.

### Notable Limitations:
- File Size: 100 MB
- Page Count: 200 pages (non-scanned, textual), 100 pages (scanned, images)
- Request Rate: 25 requests / minute

### Usage with `doc_collect.py`:
- To use the API, first download the credentials archive (ZIP) from [Adobe](https://documentcloud.adobe.com/dc-integration-creation-app-cdn/main.html?api=pdf-extract-api)
- Extract the contents of the archive.
- In the project root directory, create a subdirectory named `credentials`.
- Add the following files to the subdirectory from the extracted archive directory:
  - `pdfservices-api-credentials.json` (in `<ExtractRoot>/adobe-dc-pdf-services-sdk-extract-python-samples/`)
  - `private.key` (in `<ExtractRoot>/`)

### References:
- [https://developer.adobe.com/document-services/docs/overview/pdf-extract-api/howtos/extract-api](https://developer.adobe.com/document-services/docs/overview/pdf-extract-api/howtos/extract-api)