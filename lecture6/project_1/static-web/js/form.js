// Set 'URL' to your API Gateway endpoint
const URL = 'https://smlxsmxlrk.execute-api.us-east-1.amazonaws.com/prod';

// Attach a submit event listener to the main form
document.getElementById('mainForm').addEventListener('submit', (e) => {
  // Prevent the default action of the form
  e.preventDefault();

  // Get the values of the form fields
  const name = document.getElementById('name').value;
  const email = document.getElementById('email').value;
  const phone = document.getElementById('phone').value;
  const address = document.getElementById('address').value;
  const programming_languages = document.getElementById('programming_languages').value;
  const tools = document.getElementById('tools').value;

  // Create a JSON object of the form data
  const data = {
    name,
    email,
    phone,
    address,
    programming_languages,
    tools
  };

  // Make an AJAX request to the API Gateway endpoint
  fetch(URL, {
    method: 'POST',
    mode: 'cors',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    body: JSON.stringify(data)
  })
    .then(response => {
      if (response.ok) {
        document.getElementById('form-response').textContent = 'Form submitted!';
        document.getElementById('form-response').style.color = 'green';
        document.getElementById('form-response').style.fontWeight = 'bold';
      } else {
        throw new Error('Error occurred. Status: ' + response.status);
      }
    })
    .catch(error => {
      console.log("Post error: " + error);
      document.getElementById('form-response').textContent = 'Error: ' + error.message;
      document.getElementById('form-response').style.color = 'red';
      document.getElementById('form-response').style.fontWeight = 'bold';
    });
});
