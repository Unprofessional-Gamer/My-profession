import PyPDF2

def unlock_pdf(input_pdf_path, output_pdf_path, password):
    try:
        # Open the encrypted PDF file
        with open(input_pdf_path, 'rb') as input_file:
            reader = PyPDF2.PdfReader(input_file)
            
            # Check if the PDF is encrypted
            if reader.is_encrypted:
                # Try to decrypt the PDF with the provided password
                if reader.decrypt(password):
                    print("PDF successfully decrypted.")
                else:
                    print("Failed to decrypt PDF. Incorrect password.")
                    return

            # Create a PdfWriter object to write the decrypted PDF
            writer = PyPDF2.PdfWriter()

            # Add all pages from the reader to the writer
            for page_num in range(len(reader.pages)):
                writer.add_page(reader.pages[page_num])

            # Write the decrypted content to the output PDF
            with open(output_pdf_path, 'wb') as output_file:
                writer.write(output_file)

            print(f"Decrypted PDF saved as '{output_pdf_path}'.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Path to the encrypted PDF
input_pdf_path = 'path/to/your/encrypted.pdf'
# Path to the output decrypted PDF
output_pdf_path = 'path/to/your/decrypted.pdf'
# Password for the encrypted PDF
password = 'your_password_here'

unlock_pdf(input_pdf_path, output_pdf_path, password)